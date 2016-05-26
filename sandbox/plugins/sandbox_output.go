/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mike Trinkala (trink@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package plugins

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	. "github.com/mozilla-services/heka/sandbox"
	"github.com/mozilla-services/heka/sandbox/lua"
)

// Heka Output plugin that acts as a wrapper for sandboxed output scripts.
// Each sandboxed output maps to exactly one SandboxOutput instance.
type SandboxOutput struct {
	processMessageCount    int64
	processMessageFailures int64
	processMessageSamples  int64
	processMessageDuration int64
	timerEventSamples      int64
	timerEventDuration     int64

	sb                Sandbox
	sbc               *SandboxConfig
	preservationFile  string
	reportLock        sync.Mutex
	name              string
	pConfig           *pipeline.PipelineConfig
	sample            bool
	sampleDenominator int
}

// Heka will call this before calling any other methods to give us access to
// the pipeline configuration.
func (s *SandboxOutput) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	s.pConfig = pConfig
}

func (s *SandboxOutput) ConfigStruct() interface{} {
	return NewSandboxConfig(s.pConfig.Globals)
}

func (s *SandboxOutput) Init(config interface{}) (err error) {
	s.sbc = config.(*SandboxConfig)
	globals := s.pConfig.Globals
	s.sbc.ScriptFilename = globals.PrependShareDir(s.sbc.ScriptFilename)
	s.sbc.InstructionLimit = 0
	s.sbc.PluginType = "output"

	data_dir := globals.PrependBaseDir(DATA_DIR)
	if !fileExists(data_dir) {
		err = os.MkdirAll(data_dir, 0700)
		if err != nil {
			return
		}
	}

	s.preservationFile = filepath.Join(data_dir, s.name+DATA_EXT)
	switch s.sbc.ScriptType {
	case "lua":
		stateFile := ""
		if s.sbc.PreserveData && fileExists(s.preservationFile) {
			stateFile = s.preservationFile
		}
		s.sb, err = lua.CreateLuaSandbox(s.sbc, stateFile)
		if err != nil {
			return
		}
	default:
		return fmt.Errorf("unsupported script type: %s", s.sbc.ScriptType)
	}

	s.sample = true
	s.sampleDenominator = globals.SampleDenominator
	return
}

func (s *SandboxOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {
	var (
		pack      *pipeline.PipelinePack
		retval    int
		inChan    = or.InChan()
		duration  int64
		startTime time.Time
		ok        = true
		ticker    = or.Ticker()
	)
	// Pass UpdateCursor function in to the sandbox for checkpoint management.
	s.sb.UpdateCursor(or.UpdateCursor)
	// Flag to track if we're in a batch or an async grouping.
	inBatch := false
	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				break
			}
			if s.sample {
				startTime = time.Now()
			}
			retval = s.sb.ProcessMessage(pack)
			if s.sample {
				duration = time.Since(startTime).Nanoseconds()
				s.reportLock.Lock()
				s.processMessageDuration += duration
				s.processMessageSamples++
				s.reportLock.Unlock()
			}
			s.sample = 0 == rand.Intn(s.sampleDenominator)

			switch {
			case retval == 0: // Success.
				inBatch = false
				or.UpdateCursor(pack.QueueCursor)
				atomic.AddInt64(&s.processMessageCount, 1)
				pack.Recycle(nil)
			case retval == -2: // Skip.
				// Only update the checkpoint if we're not in a batch.
				if !inBatch {
					or.UpdateCursor(pack.QueueCursor)
				}
				pack.Recycle(nil)
			case retval == -3: // Retry.
				err = pipeline.NewRetryMessageError("Retrying...")
				pack.Recycle(err)
			case retval == -4: // Batching.
				inBatch = true
				pack.Recycle(nil)
			case retval == -5: // Async.
				inBatch = true
				pack.Recycle(nil)
			case retval > 0: // Fatal error.
				err = fmt.Errorf("FATAL: %s", s.sb.LastError())
				pack.Recycle(err)
				ok = false
			default: // retval == -1 or retval < -5 => Non-fatal failure.
				atomic.AddInt64(&s.processMessageFailures, 1)
				// Only update the checkpoint if we're not in a batch.
				if !inBatch {
					or.UpdateCursor(pack.QueueCursor)
				}
				var e error
				em := s.sb.LastError()
				if len(em) > 0 {
					e = errors.New(em)
				}
				pack.Recycle(e)
			}

		case t := <-ticker:
			startTime = time.Now()
			if retval = s.sb.TimerEvent(t.UnixNano()); retval != 0 {
				err = fmt.Errorf("FATAL: %s", s.sb.LastError())
				ok = false
			}
			duration = time.Since(startTime).Nanoseconds()
			s.reportLock.Lock()
			s.timerEventDuration += duration
			s.timerEventSamples++
			s.reportLock.Unlock()
		}
	}

	if err == nil && s.sbc.TimerEventOnShutdown {
		if retval = s.sb.TimerEvent(time.Now().UnixNano()); retval != 0 {
			err = fmt.Errorf("FATAL: %s", s.sb.LastError())
		}
	}

	destroyErr := s.destroy()
	if destroyErr != nil {
		if err != nil {
			or.LogError(err)
		}
		err = destroyErr
	}
	return err
}

func (s *SandboxOutput) destroy() error {
	var err error
	s.reportLock.Lock()
	if s.sb != nil {
		err = s.sb.Destroy()
		s.sb = nil
	}
	s.reportLock.Unlock()
	return err
}

// Satisfies the `pipeline.ReportingPlugin` interface to provide sandbox state
// information to the Heka report and dashboard.
func (s *SandboxOutput) ReportMsg(msg *message.Message) error {
	s.reportLock.Lock()
	defer s.reportLock.Unlock()

	if s.sb == nil {
		return fmt.Errorf("Output is not running")
	}

	stats := s.sb.Stats()
	message.NewIntField(msg, "Memory", stats.MemCur, "B")
	message.NewIntField(msg, "MaxMemory", stats.MemMax, "B")
	message.NewIntField(msg, "MaxInstructions", stats.InstruxMax, "count")

	message.NewInt64Field(msg, "ProcessMessageCount", atomic.LoadInt64(&s.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures", atomic.LoadInt64(&s.processMessageFailures), "count")
	message.NewInt64Field(msg, "ProcessMessageSamples", s.processMessageSamples, "count")
	message.NewInt64Field(msg, "TimerEventSamples", s.timerEventSamples, "count")

	var tmp int64 = 0
	if s.processMessageSamples > 0 {
		tmp = s.processMessageDuration / s.processMessageSamples
	}
	message.NewInt64Field(msg, "ProcessMessageAvgDuration", tmp, "ns")

	tmp = 0
	if s.timerEventSamples > 0 {
		tmp = s.timerEventDuration / s.timerEventSamples
	}
	message.NewInt64Field(msg, "TimerEventAvgDuration", tmp, "ns")

	return nil
}

func init() {
	pipeline.RegisterPlugin("SandboxOutput", func() interface{} {
		return new(SandboxOutput)
	})
}
