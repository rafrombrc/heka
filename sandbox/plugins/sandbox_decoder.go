/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2013-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mike Trinkala (trink@mozilla.com)
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package plugins

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	. "github.com/mozilla-services/heka/sandbox"
	"github.com/mozilla-services/heka/sandbox/lua"
	"github.com/pborman/uuid"
)

// Decoder for converting structured/unstructured data into Heka messages.
type SandboxDecoder struct {
	processMessageCount    int64
	processMessageFailures int64
	processMessageSamples  int64
	processMessageDuration int64
	sb                     Sandbox
	sbc                    *SandboxConfig
	preservationFile       string
	reportLock             sync.Mutex
	sample                 bool
	pack                   *pipeline.PipelinePack
	packs                  []*pipeline.PipelinePack
	dRunner                pipeline.DecoderRunner
	name                   string
	tz                     *time.Location
	sampleDenominator      int
	pConfig                *pipeline.PipelineConfig
	debug                  bool
}

func (s *SandboxDecoder) ConfigStruct() interface{} {
	return NewSandboxConfig(s.pConfig.Globals)
}

func (s *SandboxDecoder) SetName(name string) {
	re := regexp.MustCompile("\\W")
	s.name = re.ReplaceAllString(name, "_")
}

// Heka will call this before calling any other methods to give us access to
// the pipeline configuration.
func (s *SandboxDecoder) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	s.pConfig = pConfig
}

func (s *SandboxDecoder) Init(config interface{}) (err error) {
	s.sbc = config.(*SandboxConfig)
	globals := s.pConfig.Globals
	s.sbc.ScriptFilename = globals.PrependShareDir(s.sbc.ScriptFilename)
	s.sbc.PluginType = "decoder"
	s.sampleDenominator = globals.SampleDenominator

	s.tz = time.UTC
	if tz, ok := s.sbc.Config["tz"]; ok {
		s.tz, err = time.LoadLocation(tz.(string))
		if err != nil {
			return
		}
	}

	data_dir := globals.PrependBaseDir(DATA_DIR)
	if !fileExists(data_dir) {
		err = os.MkdirAll(data_dir, 0700)
		if err != nil {
			return
		}
	}

	switch s.sbc.ScriptType {
	case "lua":
	default:
		return fmt.Errorf("unsupported script type: %s", s.sbc.ScriptType)
	}

	s.sample = true
	return
}

func copyMessageHeaders(dst *message.Message, src *message.Message) {
	if src == nil || dst == nil || src == dst {
		return
	}

	if src.Timestamp != nil {
		dst.SetTimestamp(*src.Timestamp)
	} else {
		dst.Timestamp = nil
	}
	if src.Type != nil {
		dst.SetType(*src.Type)
	} else {
		dst.Type = nil
	}
	if src.Logger != nil {
		dst.SetLogger(*src.Logger)
	} else {
		dst.Logger = nil
	}
	if src.Severity != nil {
		dst.SetSeverity(*src.Severity)
	} else {
		dst.Severity = nil
	}
	if src.Pid != nil {
		dst.SetPid(*src.Pid)
	} else {
		dst.Pid = nil
	}
	if src.Hostname != nil {
		dst.SetHostname(*src.Hostname)
	} else {
		dst.Hostname = nil
	}
}

func copyPayloadFields(dst *message.Message, src *message.Message) error {
	dst.SetPayload(src.GetPayload())
	fPayloadTypeSrc := src.FindFirstField("payload_type")
	payloadTypes := fPayloadTypeSrc.GetValueString()
	if len(payloadTypes) > 0 {
		fPayloadType, err := message.NewField("payload_type", payloadTypes[0], "")
		if err != nil {
			return err
		}
		dst.AddField(fPayloadType)
	}
	fPayloadNameSrc := src.FindFirstField("payload_name")
	payloadNames := fPayloadNameSrc.GetValueString()
	if len(payloadNames) > 0 {
		fPayloadName, err := message.NewField("payload_name", payloadNames[0], "")
		if err != nil {
			return err
		}
		dst.AddField(fPayloadName)
	}
	return nil
}

func populateMissingHeaders(newMsg *message.Message, original *message.Message) (changed bool) {
	if newMsg.Uuid == nil {
		newMsg.SetUuid(uuid.NewRandom()) // UUID should always be unique
		changed = true
	}
	if newMsg.Timestamp == nil {
		newMsg.SetTimestamp(original.GetTimestamp())
		changed = true
	}
	if newMsg.Type == nil || *newMsg.Type == "inject_payload" {
		newMsg.SetType(original.GetType())
		changed = true
	}
	if newMsg.Hostname == nil {
		newMsg.SetHostname(original.GetHostname())
		changed = true
	}
	if newMsg.Logger == nil {
		newMsg.SetLogger(original.GetLogger())
		changed = true
	}
	if newMsg.Severity == nil {
		newMsg.SetSeverity(original.GetSeverity())
		changed = true
	}
	if newMsg.Pid == nil {
		newMsg.SetPid(original.GetPid())
		changed = true
	}
	return changed
}

func (s *SandboxDecoder) adjustTimestamp(pack *pipeline.PipelinePack) {
	const layout = "2006-01-02T15:04:05.999999999" // remove the incorrect UTC tz info
	t := time.Unix(0, pack.Message.GetTimestamp())
	t = t.In(time.UTC)
	ct, _ := time.ParseInLocation(layout, t.Format(layout), s.tz)
	pack.Message.SetTimestamp(ct.UnixNano())
	pack.TrustMsgBytes = false
}

func (s *SandboxDecoder) SetDecoderRunner(dr pipeline.DecoderRunner) {
	if s.sb != nil {
		return // no-op already initialized
	}

	s.dRunner = dr
	var original *message.Message
	var err error

	s.preservationFile = filepath.Join(s.pConfig.Globals.PrependBaseDir(DATA_DIR),
		dr.Name()+DATA_EXT)

	switch s.sbc.ScriptType {
	case "lua":
		stateFile := ""
		if s.sbc.PreserveData {
			stateFile = s.preservationFile
		}
		s.sb, err = lua.CreateLuaSandbox(s.sbc, stateFile)
	default:
		err = fmt.Errorf("unsupported script type: %s", s.sbc.ScriptType)
	}

	if err != nil {
		dr.LogError(err)
		if s.sb != nil {
			s.sb.Destroy()
			s.sb = nil
		}
		s.pConfig.Globals.ShutDown(1)
		return
	}

	s.sb.InjectMessage(func(payload string) int {
		first := false
		// s.pack == nil implies this is not the first message injection
		// triggered by the current input message.
		if s.pack == nil {
			s.pack = dr.NewPack()
			if s.pack == nil {
				return 5 // We're aborting, exit out.
			}
		} else {
			first = true
			// Save off the header values in case they get wiped out.
			original = new(message.Message)
			copyMessageHeaders(original, s.pack.Message)
		}

		// Write returned protobuf data to MsgBytes and decode.
		needed := len(payload)
		if cap(s.pack.MsgBytes) < needed {
			s.pack.MsgBytes = []byte(payload)
		} else {
			s.pack.MsgBytes = s.pack.MsgBytes[:len(payload)]
			copy(s.pack.MsgBytes, payload)
		}
		fromSbxMsg := new(message.Message)
		if proto.Unmarshal(s.pack.MsgBytes, fromSbxMsg) != nil {
			return 1
		}

		// No payload_type field implies inject_message was used.
		if fromSbxMsg.FindFirstField("payload_type") == nil {
			s.pack.Message = fromSbxMsg
			// If injections fail to set the standard headers, use the values
			// from the original message.
			s.pack.TrustMsgBytes = !populateMissingHeaders(s.pack.Message, original)
			if s.tz != time.UTC {
				s.adjustTimestamp(s.pack)
			}
			s.packs = append(s.packs, s.pack)
			s.pack = nil
			return 0
		}

		// If we got this far then inject_payload was used.
		s.pack.TrustMsgBytes = false
		if first {
			// Copy payload, payload_type, and payload_name values back to
			// the input message.
			err := copyPayloadFields(s.pack.Message, fromSbxMsg)
			if err != nil {
				return 1
			}
		} else {
			// Populate message headers from the original input message.
			populateMissingHeaders(fromSbxMsg, original)
			s.pack.Message = fromSbxMsg
		}

		if s.tz != time.UTC {
			s.adjustTimestamp(s.pack)
		}

		s.packs = append(s.packs, s.pack)
		s.pack = nil
		return 0
	})
}

func (s *SandboxDecoder) Shutdown() {
	err := s.destroy()
	if err != nil {
		s.dRunner.LogError(err)
	}
}

func (s *SandboxDecoder) destroy() error {
	s.reportLock.Lock()

	var err error
	if s.sb != nil {
		err = s.sb.Destroy()
		s.sb = nil
	}
	s.reportLock.Unlock()
	return err
}

func (s *SandboxDecoder) Decode(pack *pipeline.PipelinePack) (packs []*pipeline.PipelinePack,
	err error) {

	if s.sb == nil {
		err = fmt.Errorf("SandboxDecoder has been terminated")
		return
	}
	s.pack = pack
	atomic.AddInt64(&s.processMessageCount, 1)

	var startTime time.Time
	if s.sample {
		startTime = time.Now()
	}
	retval := s.sb.ProcessMessage(s.pack)
	if s.sample {
		duration := time.Since(startTime).Nanoseconds()
		s.reportLock.Lock()
		s.processMessageDuration += duration
		s.processMessageSamples++
		s.reportLock.Unlock()
	}
	s.sample = 0 == rand.Intn(s.sampleDenominator)
	if retval > 0 {
		err = fmt.Errorf("FATAL (%d): %s", retval, s.sb.LastError())
		s.dRunner.LogError(err)
		s.pConfig.Globals.ShutDown(1)
	}
	if retval < 0 {
		atomic.AddInt64(&s.processMessageFailures, 1)
		if s.pack != nil {
			err = fmt.Errorf("Failed parsing: %s payload: %s",
				s.sb.LastError(), s.pack.Message.GetPayload())
		} else {
			err = fmt.Errorf("Failed after a successful inject_message call: %s", s.sb.LastError())
		}
		if len(s.packs) > 1 {
			for _, p := range s.packs[1:] {
				p.Recycle(nil)
			}
		}
		s.packs = nil
	}
	if retval == 0 && s.pack != nil {
		// InjectMessage was never called, we're passing the original message
		// through.
		packs = append(packs, pack)
		s.pack = nil
	} else {
		packs = s.packs
	}
	s.packs = nil
	return packs, err
}

func (s *SandboxDecoder) EncodesMsgBytes() bool {
	return true
}

// Satisfies the `pipeline.ReportingPlugin` interface to provide sandbox state
// information to the Heka report and dashboard.
func (s *SandboxDecoder) ReportMsg(msg *message.Message) error {
	s.reportLock.Lock()
	defer s.reportLock.Unlock()

	if s.sb == nil {
		return fmt.Errorf("Decoder is not running")
	}

	stats := s.sb.Stats()

	message.NewIntField(msg, "Memory", stats.MemCur, "B")
	message.NewIntField(msg, "MaxMemory", stats.MemMax, "B")
	message.NewIntField(msg, "MaxInstructions", stats.InstruxMax, "count")
	message.NewIntField(msg, "MaxOutput", stats.OutputMax, "B")
	message.NewInt64Field(msg, "ProcessMessageCount", atomic.LoadInt64(&s.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures", atomic.LoadInt64(&s.processMessageFailures), "count")
	message.NewInt64Field(msg, "ProcessMessageSamples", s.processMessageSamples, "count")

	var tmp int64 = 0
	if s.processMessageSamples > 0 {
		tmp = s.processMessageDuration / s.processMessageSamples
	}
	message.NewInt64Field(msg, "ProcessMessageAvgDuration", tmp, "ns")

	return nil
}

func init() {
	pipeline.RegisterPlugin("SandboxDecoder", func() interface{} {
		return new(SandboxDecoder)
	})
	pipeline.RegisterPlugin("SandboxFilter", func() interface{} {
		return new(SandboxFilter)
	})
	pipeline.RegisterPlugin("SandboxManagerFilter", func() interface{} {
		return new(SandboxManagerFilter)
	})
}
