/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mike Trinkala (trink@mozilla.com)
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/
package lua_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	. "github.com/mozilla-services/heka/sandbox"
	"github.com/mozilla-services/heka/sandbox/lua"
	"github.com/pborman/uuid"
)

func TestCreation(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/hello_world.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 1024
	sbc.PluginType = "filter"
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	stats := sb.Stats()
	b := stats.MemCur
	if b == 0 {
		t.Errorf("current memory should be >0, using %d", b)
	}
	b = stats.MemMax
	if b == 0 {
		t.Errorf("maximum memory should be >0, using %d", b)
	}
	b = stats.InstruxMax
	if b != 3 {
		t.Errorf("maximum instructions should be 3, using %d", b)
	}
	b = stats.OutputMax
	if b != 0 {
		t.Errorf("maximum output should be 0, using %d", b)
	}
	if sb.LastError() != "" {
		t.Errorf("LastError() should be empty, received: %s", sb.LastError())
	}
	sb.Destroy()
}

func TestFailedInit(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/missing.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	_, err := lua.CreateLuaSandbox(&sbc, "")
	if err == nil {
		t.Errorf("CreateLuaSandbox should have failed on a missing file")
	}
}

func TestMissingProcessMessage(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/no_process_message.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 1024
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	r := sb.ProcessMessage(pack)
	if r == 0 {
		t.Errorf("ProcessMessage() expected: 1, received: %d", r)
	}
	s := "process_message() function was not found"
	if sb.LastError() != s {
		t.Errorf("LastError() should be \"%s\", received: \"%s\"", s, sb.LastError())
	}
	if STATUS_TERMINATED != sb.Status() {
		t.Errorf("status should be %d, received %d",
			STATUS_TERMINATED, sb.Status())
	}
	r = sb.ProcessMessage(pack) // try to use the terminated plugin
	if r == 0 {
		t.Errorf("ProcessMessage() expected: 1, received: %d", r)
	}
	sb.Destroy()
}

func TestMissingTimeEvent(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/hello_world.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 1024
	sbc.PluginType = "filter"
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	r := sb.TimerEvent(time.Now().UnixNano())
	if r == 0 {
		t.Errorf("TimerEvent() expected: 1, received: %d", r)
	}
	if STATUS_TERMINATED != sb.Status() {
		t.Errorf("status should be %d, received %d",
			STATUS_TERMINATED, sb.Status())
	}
	r = sb.TimerEvent(time.Now().UnixNano()) // try to use the terminated plugin
	if r == 0 {
		t.Errorf("TimerEvent() expected: 1, received: %d", r)
	}
	sb.Destroy()
}

func getTestMessage() *message.Message {
	hostname, _ := os.Hostname()
	field, _ := message.NewField("foo", "bar", "")
	msg := &message.Message{}
	msg.SetType("TEST")
	msg.SetTimestamp(5123456789)
	msg.SetPid(9283)
	msg.SetUuid(uuid.NewRandom())
	msg.SetLogger("GoSpec")
	msg.SetSeverity(int32(6))
	msg.SetEnvVersion("0.8")
	msg.SetPid(int32(os.Getpid()))
	msg.SetHostname(hostname)
	msg.AddField(field)

	var emptyByte []byte
	data := []byte("data")
	field1, _ := message.NewField("bytes", data, "")
	field2, _ := message.NewField("int", int64(999), "")
	field2.AddValue(int64(1024))
	field3, _ := message.NewField("double", float64(99.9), "")
	field4, _ := message.NewField("bool", true, "")
	field5, _ := message.NewField("foo", "alternate", "")
	field6, _ := message.NewField("false", false, "")
	field7, _ := message.NewField("empty_bytes", emptyByte, "")
	msg.AddField(field1)
	msg.AddField(field2)
	msg.AddField(field3)
	msg.AddField(field4)
	msg.AddField(field5)
	msg.AddField(field6)
	msg.AddField(field7)
	return msg
}

func getTestPack() *pipeline.PipelinePack {
	pack := pipeline.NewPipelinePack(nil)
	pack.Message = getTestMessage()
	return pack
}

func TestProcessMessage(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/hello_world.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 1024
	sbc.PluginType = "filter"
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	pack := getTestPack()
	sb.InjectMessage(func(p string) int {
		return 0
	})
	sb.ProcessMessage(pack)
	stats := sb.Stats()
	b := stats.MemCur
	if b == 0 {
		t.Errorf("current memory should be >0, using %d", b)
	}
	b = stats.MemMax
	if b == 0 {
		t.Errorf("maximum memory should be >0, using %d", b)
	}
	b = stats.InstruxMax
	if b != 7 {
		t.Errorf("maximum instructions should be 7, using %d", b)
	}
	b = stats.OutputMax
	if b != 12 {
		t.Errorf("maximum output should be 12, using %d", b)
	}
	if STATUS_RUNNING != sb.Status() {
		t.Errorf("status should be %d, received %d",
			STATUS_RUNNING, sb.Status())
	}
	sb.Destroy()
}

func TestAPIErrors(t *testing.T) {
	pack := getTestPack()
	tests := []string{
		"require unknown",
		"add_to_payload() no arg",
		"out of memory",
		"out of instructions",
		"operation on a nil",
		"invalid return",
		"no return",
		"read_message() incorrect number of args",
		"read_message() incorrect field name type",
		"read_message() negative field index",
		"read_message() negative array index",
		"output limit exceeded",
		"read_config() must have a single argument",
		"write_message() should not exist",
		"invalid error message",
	}
	msgs := []string{
		"process_message() ./testsupport/errors.lua:11: module 'unknown' not found:\n\tno file 'unknown.lua'\n\tno file 'unknown.so'",
		"process_message() ./testsupport/errors.lua:13: bad argument #0 to 'add_to_payload' (must have at least one argument)",
		"process_message() not enough memory",
		"process_message() instruction_limit exceeded",
		"process_message() ./testsupport/errors.lua:22: attempt to perform arithmetic on global 'x' (a nil value)",
		"process_message() must return a numeric status code",
		"process_message() must return a numeric status code",
		"process_message() ./testsupport/errors.lua:28: read_message() incorrect number of arguments",
		"process_message() ./testsupport/errors.lua:30: bad argument #1 to 'read_message' (string expected, got nil)",
		"process_message() ./testsupport/errors.lua:32: bad argument #2 to 'read_message' (field index must be >= 0)",
		"process_message() ./testsupport/errors.lua:34: bad argument #3 to 'read_message' (array index must be >= 0)",
		"process_message() ./testsupport/errors.lua:37: output_limit exceeded",
		"process_message() ./testsupport/errors.lua:40: bad argument #1 to 'read_config' (string expected, got no value)",
		"process_message() ./testsupport/errors.lua:42: attempt to call global 'write_message' (a nil value)",
		"process_message() must return a nil or string error message",
	}

	if runtime.GOOS == "windows" {
		msgs[0] = "process_message() ./testsupport/errors.lua:11: module 'unknown' not found:\n\tno file 'unknown.lua'\n\tno file 'unknown.dll'"
	}

	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/errors.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 128
	sbc.PluginType = "filter"
	for i, v := range tests {
		sb, err := lua.CreateLuaSandbox(&sbc, "")
		if err != nil {
			t.Errorf("%s", err)
		}
		pack.Message.SetPayload(v)
		pack.TrustMsgBytes = false
		r := sb.ProcessMessage(pack)
		if r != 1 || STATUS_TERMINATED != sb.Status() {
			t.Errorf("test: %s status should be %d, received %d",
				v, STATUS_TERMINATED, sb.Status())
		}
		s := sb.LastError()
		if s != msgs[i] {
			t.Errorf("test: %s error should be \"%s\", received \"%s\"",
				v, msgs[i], s)
		}
		sb.Destroy()
	}
}

func TestTimerEvent(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/errors.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	r := sb.TimerEvent(time.Now().UnixNano())
	if r != 0 || STATUS_RUNNING != sb.Status() {
		t.Errorf("status should be %d, received %d",
			STATUS_RUNNING, sb.Status())
	}
	s := sb.LastError()
	if s != "" {
		t.Errorf("there should be no error; received \"%s\"", s)
	}
	sb.Destroy()
}

func TestReadMessage(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/read_message.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	pack.Message.SetPayload("Payload Test")
	r := sb.ProcessMessage(pack)
	if r != 0 {
		t.Errorf("ProcessMessage should return 0, received %d: %s", r, sb.LastError())
	}
	r = sb.TimerEvent(time.Now().UnixNano())
	if r != 0 {
		t.Errorf("read_message should return nil in timer_event")
	}
	sb.Destroy()
}

func TestReadRaw(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/read_raw.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	pack.Message.SetPayload("Payload Test")
	r := sb.ProcessMessage(pack)
	if r != 0 {
		t.Errorf("ProcessMessage should return 0, received %d last error: %s", r,
			sb.LastError())
	}
	sb.Destroy()
}

func TestRestore(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/simple_count.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 1024
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "./testsupport/simple_count.lua.data")
	if err != nil {
		t.Errorf("%s", err)
	}
	sb.InjectMessage(func(p string) int {
		msg := new(message.Message)
		err := proto.Unmarshal([]byte(p), msg)
		if err != nil {
			t.Errorf("%s", err)
		}
		if msg.GetPayload() != "10" {
			t.Errorf("State was not restored, expected 10, got %s", msg.GetPayload())
		}
		return 0
	})
	r := sb.ProcessMessage(pack)
	if r != 0 {
		t.Errorf("ProcessMessage should return 0, received %d", r)
	}
	sb.Destroy()
}

func TestRestoreMissingData(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/simple_count.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	sb, err := lua.CreateLuaSandbox(&sbc, "./testsupport/missing.data")
	if err != nil {
		t.Errorf("%s", err)
	}
	sb.Destroy()
}

func TestPreserveFailure(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/serialize_failure.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	output := filepath.Join(os.TempDir(), "serialize_failure.lua.data")
	sb, err := lua.CreateLuaSandbox(&sbc, output)
	if err != nil {
		t.Errorf("%s", err)
	}
	err = sb.Destroy()
	if err == nil {
		t.Errorf("The key of type 'function' should have failed")
	} else {
		expect := "Destroy() serialize_data cannot preserve type 'function'"
		if err.Error() != expect {
			t.Errorf("expected '%s' got '%s'", expect, err)
		}
	}
	_, err = os.Stat(output)
	if err == nil {
		t.Errorf("The output file should be removed on failure")
	}
}

func TestFailedMessageInjection(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/loop.lua"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 1024
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	sb.InjectMessage(func(p string) int {
		return 3
	})
	r := sb.ProcessMessage(pack)
	if r != 1 {
		t.Errorf("ProcessMessage should return 1, received %d", r)
	}
	if STATUS_TERMINATED != sb.Status() {
		t.Errorf("status should be %d, received %d",
			STATUS_TERMINATED, sb.Status())
	}
	s := sb.LastError()
	errMsg := "process_message() ./testsupport/loop.lua:6: inject_message() failed: rejected by the callback"
	if s != errMsg {
		t.Errorf("error should be \"%s\", received \"%s\"", errMsg, s)
	}
	sb.Destroy()
}

func TestInjectPayload(t *testing.T) {
	var sbc SandboxConfig
	tests := []string{
		"lua types",
		"cloudwatch metric",
		"external reference",
		"array only",
		"private keys",
		"special characters",
		"internal reference",
	}
	outputs := []string{
		`{"value":1}1.2 string nil true false`,
		`{"StatisticValues":[{"Minimum":0,"SampleCount":0,"Sum":0,"Maximum":0},{"Minimum":0,"SampleCount":0,"Sum":0,"Maximum":0}],"Dimensions":[{"Name":"d1","Value":"v1"},{"Name":"d2","Value":"v2"}],"MetricName":"example","Timestamp":0,"Value":0,"Unit":"s"}`,
		`{"a":{"y":2,"x":1}}`,
		`[1,2,3]`,
		`{"x":1,"_m":1,"_private":[1,2]}`,
		`{"special\tcharacters":"\"\t\r\n\b\f\\\/"}`,
		`{"y":[2],"x":[1,2,3],"ir":[1,2,3]}`,
	}
	if false { // lua jit values
		outputs[1] = `{"Timestamp":0,"Value":0,"StatisticValues":[{"SampleCount":0,"Sum":0,"Maximum":0,"Minimum":0},{"SampleCount":0,"Sum":0,"Maximum":0,"Minimum":0}],"Unit":"s","MetricName":"example","Dimensions":[{"Name":"d1","Value":"v1"},{"Name":"d2","Value":"v2"}]}`
	}

	sbc.ScriptFilename = "./testsupport/inject_message.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 8000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	cnt := 0
	sb.InjectMessage(func(p string) int {
		msg := new(message.Message)
		err := proto.Unmarshal([]byte(p), msg)
		if err != nil {
			t.Errorf("%s", err)
		}
		p = msg.GetPayload()
		if p != outputs[cnt] { // ignore the UUID
			t.Errorf("Output is incorrect, expected: \"%x\" received: \"%x\"", outputs[cnt], p)
		}
		cnt++
		return 0
	})

	for _, v := range tests {
		pack.Message.SetPayload(v)
		pack.TrustMsgBytes = false
		r := sb.ProcessMessage(pack)
		if r != 0 {
			t.Errorf("ProcessMessage should return 0, received %d %s", r, sb.LastError())
		}
	}
	sb.Destroy()
	if cnt != len(tests) {
		t.Errorf("InjectMessage was called %d times, expected %d", cnt, len(tests))
	}
}

func TestInjectMessage(t *testing.T) {
	var sbc SandboxConfig
	tests := []string{
		"message field all types",
		"round trip",
	}
	outputs := []string{
		"\x10\x80\x94\xeb\xdc\x03\x4a\x04\x74\x68\x6f\x72\x52\x13\x0a\x06\x6e\x75\x6d\x62\x65\x72\x10\x03\x39\x00\x00\x00\x00\x00\x00\xf0\x3f\x52\x2c\x0a\x07\x6e\x75\x6d\x62\x65\x72\x73\x10\x03\x1a\x05\x63\x6f\x75\x6e\x74\x3a\x18\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x08\x40\x52\x0e\x0a\x05\x62\x6f\x6f\x6c\x73\x10\x04\x42\x03\x01\x00\x00\x52\x0a\x0a\x04\x62\x6f\x6f\x6c\x10\x04\x40\x01\x52\x10\x0a\x06\x73\x74\x72\x69\x6e\x67\x22\x06\x73\x74\x72\x69\x6e\x67\x52\x15\x0a\x07\x73\x74\x72\x69\x6e\x67\x73\x22\x02\x73\x31\x22\x02\x73\x32\x22\x02\x73\x33",
		"\x10\x80\x94\xeb\xdc\x03\x4a\x04\x74\x68\x6f\x72\x52\x1b\x0a\x05\x63\x6f\x75\x6e\x74\x10\x03\x3a\x10\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f",
	}

	sbc.ScriptFilename = "./testsupport/inject_message.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 8000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	cnt := 0
	sb.InjectMessage(func(p string) int {
		if p[18:] != outputs[cnt] { // ignore the UUID
			t.Errorf("Output is incorrect, expected: \"%x\" received: \"%x\"", outputs[cnt], p[18:])
		}
		msg := new(message.Message)
		err := proto.Unmarshal([]byte(p), msg)
		if err != nil {
			t.Errorf("%s", err)
		}
		if cnt == 0 {
			if msg.GetTimestamp() != 1e9 {
				t.Errorf("Timestamp expected %d received %d", int(1e9), pack.Message.GetTimestamp())
			}
			if field := msg.FindFirstField("numbers"); field != nil {
				if field.GetRepresentation() != "count" {
					t.Errorf("'numbers' representation expected \"count\" received \"%s\"", field.GetRepresentation())
				}
			} else {
				t.Errorf("'numbers' field not found")
			}
			tests := []string{
				"Timestamp == 1000000000",
				"Fields[number] == 1",
				"Fields[numbers][0][0] == 1 && Fields[numbers][0][1] == 2 && Fields[numbers][0][2] == 3",
				"Fields[string] == 'string'",
				"Fields[strings][0][0] == 's1' && Fields[strings][0][1] == 's2' && Fields[strings][0][2] == 's3'",
				"Fields[bool] == TRUE",
				"Fields[bools][0][0] == TRUE && Fields[bools][0][1] == FALSE && Fields[bools][0][2] == FALSE",
			}
			for _, v := range tests {
				ms, _ := message.CreateMatcherSpecification(v)
				match := ms.Match(msg)
				if !match {
					t.Errorf("Test failed %s", v)
				}
			}
		}
		cnt++
		return 0
	})

	for _, v := range tests {
		pack.Message.SetPayload(v)
		pack.TrustMsgBytes = false
		r := sb.ProcessMessage(pack)
		if r != 0 {
			t.Errorf("ProcessMessage should return 0, received %d %s", r, sb.LastError())
		}
	}
	sb.Destroy()
	if cnt != len(tests) {
		t.Errorf("InjectMessage was called %d times, expected %d", cnt, len(tests))
	}
}

func TestInjectMessageError(t *testing.T) {
	var sbc SandboxConfig
	tests := []string{
		"error circular reference",
		"error escape overflow",
		"error mis-match field array",
		"error nil field",
		"error nil type arg",
		"error nil name arg",
		"error nil message",
		"error userdata output_limit",
	}
	errors := []string{
		"process_message() ./testsupport/inject_message.lua:38: Cannot serialise, excessive nesting (1001)",
		"process_message() ./testsupport/inject_message.lua:44: strbuf output_limit exceeded",
		"process_message() ./testsupport/inject_message.lua:50: inject_message() failed: array has mixed types",
		"process_message() ./testsupport/inject_message.lua:53: inject_message() failed: unsupported type: nil",
		"process_message() ./testsupport/inject_message.lua:55: inject_payload() payload_type argument must be a string",
		"process_message() ./testsupport/inject_message.lua:57: inject_payload() payload_name argument must be a string",
		"process_message() ./testsupport/inject_message.lua:59: bad argument #1 to 'inject_message' (table expected, got nil)",
		"process_message() ./testsupport/inject_message.lua:62: output_limit exceeded",
	}

	sbc.ScriptFilename = "./testsupport/inject_message.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 1000000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 1024
	sbc.PluginType = "filter"
	pack := getTestPack()
	for i, v := range tests {
		sb, err := lua.CreateLuaSandbox(&sbc, "")
		if i == 8 {
			sb.InjectMessage(func(p string) int {
				msg := new(message.Message)
				err := proto.Unmarshal([]byte(p), msg)
				if err != nil {
					return 1
				}
				return 0
			})
		}
		if err != nil {
			t.Errorf("%s", err)
		}
		pack.Message.SetPayload(v)
		pack.TrustMsgBytes = false
		r := sb.ProcessMessage(pack)
		if r != 1 {
			t.Errorf("InjectMessageError test: %s should return 1, received %d", v, r)
		} else {
			if sb.LastError() != errors[i] {
				t.Errorf("Expected: \"%s\" received: \"%s\"", errors[i], sb.LastError())
			}
		}
		sb.Destroy()
	}
}

func TestInjectMessageRaw(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/inject_message_raw.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 8000
	sbc.PluginType = "input"
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}

	output := "\x10\x80\x94\xeb\xdc\x03\x52\x1b\x0a\x05\x63\x6f\x75\x6e\x74\x10\x03\x3a\x10\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"
	sb.InjectMessage(func(p string) int {
		if p[18:] != output { // ignore the UUID
			t.Errorf("Output is incorrect, expected: \"%x\" received: \"%x\"", output, p[18:])
		}
		return 0
	})

	r := sb.ProcessMessage(nil)
	if r != 0 {
		t.Errorf("ProcessMessage should return 0, received %d %s", r, sb.LastError())
	}
	sb.Destroy()
}

func TestInjectMessageRawError(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/inject_message_raw_error.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 8000
	sbc.PluginType = "input"
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}

	error := "process_message() ./testsupport/inject_message_raw_error.lua:6: inject_message() attempted to inject a invalid protobuf string"

	r := sb.ProcessMessage(nil)
	if r != 1 {
		t.Errorf("InjectMessageRawError test: should return 1, received %d", r)
	} else {
		if sb.LastError() != error {
			t.Errorf("Expected: \"%s\" received: \"%s\"", error, sb.LastError())
		}
	}
	sb.Destroy()
}

func TestLpeg(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/lpeg_csv.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 8000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	sb.InjectMessage(func(p string) int {
		msg := new(message.Message)
		err := proto.Unmarshal([]byte(p), msg)
		if err != nil {
			t.Errorf("Can't unmarshal injected message: %s", err)
		}
		expected := `["1","string with spaces","quoted string, with comma and \"quoted\" text"]`
		if msg.GetPayload() != expected {
			t.Errorf("Output is incorrect, expected: \"%s\" received: \"%s\"", expected, p)
		}
		return 0
	})

	pack.Message.SetPayload("1,string with spaces,\"quoted string, with comma and \"\"quoted\"\" text\"")
	r := sb.ProcessMessage(pack)
	if r != 0 {
		t.Errorf("ProcessMessage should return 0, received %d %s", r, sb.LastError())
	}
	sb.Destroy()
}

func TestReadConfig(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/read_config.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	sbc.Config = make(map[string]interface{})
	sbc.Config["string"] = "widget"
	sbc.Config["int64"] = int64(99)
	sbc.Config["double"] = 99.123
	sbc.Config["bool"] = true
	sbc.Config["array"] = []int{1, 2, 3}
	sbc.Config["object"] = map[string]string{"item": "test"}
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	sb.Destroy()
}

func TestCJson(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/cjson.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 8000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	pack.Message.SetPayload("[ true, { \"foo\": \"bar\" } ]")
	r := sb.ProcessMessage(pack)
	if r != 0 {
		t.Errorf("ProcessMessage should return 0, received %d %s", r, sb.LastError())
	}
	sb.Destroy()
}

func TestGraphiteHelpers(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/graphite.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 8000
	sbc.PluginType = "filter"
	sbc.Config = make(map[string]interface{})

	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}

	for i := 0; i < 4; i++ {
		pack := getTestPack()
		pack.Message.SetHostname("localhost")
		pack.Message.SetLogger("GoSpec")

		message.NewIntField(pack.Message, "status", 200, "status")

		message.NewIntField(pack.Message, "request_time", 15*i, "request_time")
		r := sb.ProcessMessage(pack)
		if r != 0 {
			t.Errorf("Graphite returned %s", r)
		}
	}

	injectCount := 0
	sb.InjectMessage(func(p string) int {
		msg := new(message.Message)
		err := proto.Unmarshal([]byte(p), msg)
		if err != nil {
			t.Errorf("Can't unmarshal injected message: %s", err)
		}
		graphite_payload := `stats.counters.localhost.nginx.GoSpec.http_200.count 4 0
stats.counters.localhost.nginx.GoSpec.http_200.rate 0.400000 0
stats.timers.localhost.nginx.GoSpec.request_time.count 4 0
stats.timers.localhost.nginx.GoSpec.request_time.count_ps 0.400000 0
stats.timers.localhost.nginx.GoSpec.request_time.lower 0.000000 0
stats.timers.localhost.nginx.GoSpec.request_time.upper 45.000000 0
stats.timers.localhost.nginx.GoSpec.request_time.sum 90.000000 0
stats.timers.localhost.nginx.GoSpec.request_time.mean 22.500000 0
stats.timers.localhost.nginx.GoSpec.request_time.mean_90 22.500000 0
stats.timers.localhost.nginx.GoSpec.request_time.upper_90 45.000000 0
stats.statsd.numStats 2 0
`
		payload_type, _ := msg.GetFieldValue("payload_type")
		if payload_type != "txt" {
			t.Errorf("Received payload type: %s", payload_type)
		}

		payload_name, _ := msg.GetFieldValue("payload_name")
		if payload_name != "statmetric" {
			t.Errorf("Received payload name: %s", payload_name)
		}

		payload := msg.GetPayload()
		if graphite_payload != payload {
			t.Errorf("Received payload: %s", payload)
		}
		injectCount += 1
		return 0
	})

	sb.TimerEvent(200)
	if injectCount > 0 {
		t.Errorf("Looks there was an error during timer_event")
	}
	sb.Destroy()
}

func TestReadNilConfig(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/read_config_nil.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	sb.Destroy()
}

func TestExternalModule(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/require.lua"
	sbc.ModuleDirectory = "./testsupport"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 8000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	sb.InjectMessage(func(p string) int {
		msg := new(message.Message)
		err := proto.Unmarshal([]byte(p), msg)
		if err != nil {
			t.Errorf("Can't unmarshal injected message: %s", err)
		}
		if msg.GetPayload() != "43" {
			t.Errorf("Required `constant_module` should have evaluated to 43, received '%s'",
				msg.GetPayload())
		}
		return 0
	})
	r := sb.ProcessMessage(pack)
	if r != 0 {
		t.Errorf("ProcessMessage should return 0, received %d: %s", r, sb.LastError())
	}
	sb.Destroy()
}

func TestAlert(t *testing.T) {
	var sbc SandboxConfig
	tests := []string{
		"alert1\nalert2\nalert3",
		"alert5",
		"alert8",
	}

	sbc.ScriptFilename = "./testsupport/alert.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 8000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	cnt := 0
	sb.InjectMessage(func(p string) int {
		msg := new(message.Message)
		err := proto.Unmarshal([]byte(p), msg)
		if err != nil {
			t.Errorf("Can't unmarshal injected message.")
		}

		pt, _ := msg.GetFieldValue("payload_type")
		if pt != "alert" {
			t.Errorf("Payload type, expected: \"alert\" received: \"%s\"", pt)
		}
		if msg.GetPayload() != tests[cnt] {
			t.Errorf("Output is incorrect, expected: \"%s\" received: \"%s\"", tests[cnt], p)
		}
		cnt++
		return 0
	})

	for i, _ := range tests {
		pack.Message.SetTimestamp(int64(i))
		pack.TrustMsgBytes = false
		r := sb.ProcessMessage(pack)
		if r != 0 {
			t.Errorf("ProcessMessage should return 0, received %d %s", r, sb.LastError())
		}
	}
	sb.Destroy()
	if cnt != len(tests) {
		t.Errorf("Executed %d test, expected %d", cnt, len(tests))
	}
}

func TestAnnotation(t *testing.T) {
	var sbc SandboxConfig
	tests := []string{
		"{\"annotations\":[{\"text\":\"anomaly\",\"x\":1000,\"shortText\":\"A\",\"col\":1},{\"text\":\"anomaly2\",\"x\":5000,\"shortText\":\"A\",\"col\":2},{\"text\":\"maintenance\",\"x\":60000,\"shortText\":\"M\",\"col\":1}]}\n",
		"{\"annotations\":[{\"text\":\"maintenance\",\"x\":60000,\"shortText\":\"M\",\"col\":1}]}\n",
		"{\"annotations\":{}}\n",
		"ok",
	}

	sbc.ScriptFilename = "./testsupport/annotation.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 8000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}
	cnt := 0
	sb.InjectMessage(func(p string) int {
		msg := new(message.Message)
		err := proto.Unmarshal([]byte(p), msg)
		if err != nil {
			t.Errorf("Can't unmarshal injected message.")
		}
		if msg.GetPayload() != tests[cnt] {
			t.Errorf("Output is incorrect, expected: \"%s\" received: \"%s\"", tests[cnt], p)
		}
		cnt++
		return 0
	})

	for i, _ := range tests {
		pack.Message.SetTimestamp(int64(i))
		pack.TrustMsgBytes = false
		r := sb.ProcessMessage(pack)
		if r != 0 {
			t.Errorf("ProcessMessage should return 0, received %d %s", r, sb.LastError())
		}
	}
	sb.Destroy()
	if cnt != len(tests) {
		t.Errorf("Executed %d test, expected %d", cnt, len(tests))
	}
}

func TestAnomaly(t *testing.T) {
	var sbc SandboxConfig

	sbc.ScriptFilename = "./testsupport/anomaly.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 1e6
	sbc.InstructionLimit = 1e6
	sbc.OutputLimit = 1000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Errorf("%s", err)
	}

	r := sb.ProcessMessage(pack)
	if r != 0 {
		t.Errorf("ProcessMessage should return 0, received %d %s", r, sb.LastError())
	}
	sb.Destroy()
}

func TestElasticSearch(t *testing.T) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/elasticsearch.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 1e6
	sbc.InstructionLimit = 1e6
	sbc.OutputLimit = 1000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, err := lua.CreateLuaSandbox(&sbc, "")
	if err != nil {
		t.Error(err)
	}
	r := sb.ProcessMessage(pack)
	if r != 0 {
		t.Errorf("ProcessMessage should return 0, received %d %s", r, sb.LastError())
	}
	sb.Destroy()
}

func BenchmarkSandboxCreateInitDestroy(b *testing.B) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/serialize.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	for i := 0; i < b.N; i++ {
		sb, _ := lua.CreateLuaSandbox(&sbc, "")
		sb.Destroy()
	}
}

func BenchmarkSandboxCreateInitDestroyRestore(b *testing.B) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/serialize.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	for i := 0; i < b.N; i++ {
		sb, _ := lua.CreateLuaSandbox(&sbc, "./testsupport/serialize.lua.data")
		sb.Destroy()
	}
}

func BenchmarkSandboxCreateInitDestroyPreserve(b *testing.B) {
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/serialize.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	for i := 0; i < b.N; i++ {
		sb, _ := lua.CreateLuaSandbox(&sbc, "/tmp/serialize.lua.data")
		sb.Destroy()
	}
}

func BenchmarkSandboxProcessMessageCounter(b *testing.B) {
	b.StopTimer()
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/counter.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, _ := lua.CreateLuaSandbox(&sbc, "")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sb.ProcessMessage(pack)
	}
	sb.Destroy()
}

func BenchmarkSandboxReadMessageString(b *testing.B) {
	b.StopTimer()
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/readstring.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, _ := lua.CreateLuaSandbox(&sbc, "")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sb.ProcessMessage(pack)
	}
	sb.Destroy()
}

func BenchmarkSandboxReadMessageInt(b *testing.B) {
	b.StopTimer()
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/readint.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, _ := lua.CreateLuaSandbox(&sbc, "")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sb.ProcessMessage(pack)
	}
	sb.Destroy()
}

func BenchmarkSandboxReadMessageField(b *testing.B) {
	b.StopTimer()
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/readfield.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 32767
	sbc.InstructionLimit = 1000
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, _ := lua.CreateLuaSandbox(&sbc, "")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sb.ProcessMessage(pack)
	}
	sb.Destroy()
}

func BenchmarkSandboxOutputLuaTypes(b *testing.B) {
	b.StopTimer()
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/inject_message.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 1024
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, _ := lua.CreateLuaSandbox(&sbc, "")
	sb.InjectMessage(func(p string) int {
		return 0
	})
	pack.Message.SetPayload("lua types")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sb.ProcessMessage(pack)
	}
	sb.Destroy()
}

func BenchmarkSandboxOutputTable(b *testing.B) {
	b.StopTimer()
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/inject_message.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 1024
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, _ := lua.CreateLuaSandbox(&sbc, "")
	sb.InjectMessage(func(p string) int {
		return 0
	})
	pack.Message.SetPayload("cloudwatch metric")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sb.ProcessMessage(pack)
	}
	sb.Destroy()
}

func BenchmarkSandboxOutputCbuf(b *testing.B) {
	b.StopTimer()
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/inject_message.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 64512
	sbc.PluginType = "filter"
	sb, _ := lua.CreateLuaSandbox(&sbc, "")
	sb.InjectMessage(func(p string) int {
		return 0
	})
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sb.TimerEvent(0)
	}
	sb.Destroy()
}

func BenchmarkSandboxOutputMessage(b *testing.B) {
	b.StopTimer()
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/inject_message.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 64512
	sbc.PluginType = "filter"
	sb, _ := lua.CreateLuaSandbox(&sbc, "")
	sb.InjectMessage(func(p string) int {
		return 0
	})
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sb.TimerEvent(1)
	}
	sb.Destroy()
}

func BenchmarkSandboxOutputMessageAsJSON(b *testing.B) {
	b.StopTimer()
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/inject_message.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 100000
	sbc.InstructionLimit = 1000
	sbc.OutputLimit = 64512
	sbc.PluginType = "filter"
	sb, _ := lua.CreateLuaSandbox(&sbc, "")
	sb.InjectMessage(func(p string) int {
		return 0
	})
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sb.TimerEvent(2)
	}
	sb.Destroy()
}

func BenchmarkSandboxLpegDecoder(b *testing.B) {
	b.StopTimer()
	var sbc SandboxConfig
	sbc.ScriptFilename = "./testsupport/decoder.lua"
	sbc.ModuleDirectory = "./modules"
	sbc.MemoryLimit = 1024 * 1024 * 8
	sbc.InstructionLimit = 1e6
	sbc.OutputLimit = 1024 * 63
	sbc.PluginType = "filter"
	pack := getTestPack()
	sb, _ := lua.CreateLuaSandbox(&sbc, "")
	sb.InjectMessage(func(p string) int {
		return 0
	})
	pack.Message.SetPayload("1376389920 debug id=2321 url=example.com item=1")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sb.ProcessMessage(pack)
	}
	sb.Destroy()
}
