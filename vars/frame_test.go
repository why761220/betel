package values

import (
	"encoding/json"
	"github.com/google/uuid"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func Test_Frame_0(t *testing.T) {
	col := Column{
		DataType: TypeStr,
		Name:     "traceId",
		Field:    "traceId",
		Comment:  "test",
	}
	var col2 Column
	if bs, err := json.Marshal(col); err != nil {
		t.Error(err)
	} else if err = json.Unmarshal(bs, &col2); err != nil {
		t.Error(err)
	} else {
		t.Log(string(bs))
	}
}
func Test_frame_1(t *testing.T) {
	ti := Column{Name: "timestamp", DataType: TypeTime, Fmt: "ms"}
	cols := []Column{
		{Name: "traceId", DataType: TypeStr, Index: true},
		{Name: "userId", DataType: TypeStr, Index: true},
		{Name: "userInfoOrigin", DataType: TypeInt},
		{Name: "sessionId", DataType: TypeStr},
		{Name: "sysId", DataType: TypeInt},
		{Name: "agentId", DataType: TypeInt},
		{Name: "requestURI", DataType: TypeStr},
		{Name: "parameters", DataType: TypeStr},
		{Name: "requestParams", DataType: TypeStr},
		{Name: "duration", DataType: TypeInt},
		{Name: "isError", DataType: TypeBool},
		{Name: "isLogin", DataType: TypeBool},
		{Name: "os", DataType: TypeStr},
		{Name: "ip", DataType: TypeStr},
		{Name: "serverIp", DataType: TypeStr},
		{Name: "xpath", DataType: TypeStr},
		{Name: "referer", DataType: TypeStr},
	}
	getter := map[string]func(i int) Value{
		"traceId":        func(i int) Value { return strValue(strings.ReplaceAll(uuid.New().String(), "-", "")) },
		"userId":         func(i int) Value { return strValue([]string{"abc", "why"}[i%2]) },
		"timestamp":      func(i int) Value { return intValue(time.Now().Unix()) },
		"userInfoOrigin": func(i int) Value { return intValue(i % 2) },
		"sessionId":      func(i int) Value { return strValue(strings.ReplaceAll(uuid.New().String(), "-", "")) },
		"sysId":          func(i int) Value { return intValue(i % 5) },
		"agentId":        func(i int) Value { return intValue(i % 20) },
		"requestURI":     func(i int) Value { return strValue("http://www.sina.com.cn") },
		"parameters":     func(i int) Value { return strValue("userId=abc") },
		"requestParams":  func(i int) Value { return strValue("userId=abc&test=a") },
		"duration":       func(i int) Value { return intValue(rand.Intn(1000)) },
		"isError":        func(i int) Value { return boolValue([]byte{1, 0, 1, 0, 1}[i%5]) },
		"isLogin":        func(i int) Value { return boolValue([]byte{1, 0, 1, 0, 1}[i%5]) },
		"os":             func(i int) Value { return strValue([]string{"windows", "linux"}[i%2]) },
		"ip":             func(i int) Value { return strValue("192.168.3.1") },
		"serverIp":       func(i int) Value { return strValue("192.168.3.2") },
		"xpath":          func(i int) Value { return strValue("/div/table/tr/td") },
		"referer":        func(i int) Value { return strValue("http://www.sina.com.cn") },
	}
	frame := NewFrame(&ti, nil, nil)
	var err error
	for i := 0; err == nil && i < 1000000; i++ { //214694
		err = frame.Add(func(name string) (value Value, err error) {
			return getter[name](i), nil
		}, func(iter func(key string) error) (err error) {
			if err = iter(ti.Name); err != nil {
				return
			}
			for i := range cols {
				if err = iter(cols[i].Name); err != nil {
					return
				}
			}
			return
		})
	}
	if err == nil {

		if err := frame.Save("file://C:/work/test.tar.gz"); err != nil {
			t.Log(err)
		}
	} else {
		t.Log(err)
	}
}
func Test_frame_2(t *testing.T) {
	if frame, _, err := LoadFrame("file://C:/work/test.tar.gz", ""); err != nil {
		t.Error(err)
	} else {
		t.Log(frame.count)
	}
}
func Test_frame_3(t *testing.T) {

	ti := Column{Name: "timestamp", DataType: TypeTime, Fmt: "ms"}
	dims := []Column{
		{Name: "userId", DataType: TypeStr, Index: true},
	}
	metrics := []Column{
		{Name: "traceId", DataType: TypeStr, Index: true},
		{Name: "userInfoOrigin", DataType: TypeInt},
		{Name: "sessionId", DataType: TypeStr},
		{Name: "sysId", DataType: TypeInt},
		{Name: "agentId", DataType: TypeInt},
		{Name: "requestURI", DataType: TypeStr},
		{Name: "parameters", DataType: TypeStr},
		{Name: "requestParams", DataType: TypeStr},
		{Name: "duration", DataType: TypeInt},
		{Name: "isError", DataType: TypeBool},
		{Name: "isLogin", DataType: TypeBool},
		{Name: "os", DataType: TypeStr},
		{Name: "ip", DataType: TypeStr},
		{Name: "serverIp", DataType: TypeStr},
		{Name: "xpath", DataType: TypeStr},
		{Name: "referer", DataType: TypeStr},
		{Name: "max", Field: "duration", DataType: TypeIntMax},
		{Name: "min", Field: "duration", DataType: TypeIntMin},
		{Name: "avg", Field: "duration", DataType: TypeIntAvg},
	}
	tv := time.Now().Unix()
	getter := map[string]func(i int) Value{
		"traceId":        func(i int) Value { return strValue(strings.ReplaceAll(uuid.New().String(), "-", "")) },
		"userId":         func(i int) Value { return strValue([]string{"abc", "why"}[i%2]) },
		"timestamp":      func(i int) Value { return intValue(tv) },
		"userInfoOrigin": func(i int) Value { return intValue(i % 2) },
		"sessionId":      func(i int) Value { return strValue(strings.ReplaceAll(uuid.New().String(), "-", "")) },
		"sysId":          func(i int) Value { return intValue(i % 5) },
		"agentId":        func(i int) Value { return intValue(i % 20) },
		"requestURI":     func(i int) Value { return strValue("http://www.sina.com.cn") },
		"parameters":     func(i int) Value { return strValue("userId=abc") },
		"requestParams":  func(i int) Value { return strValue("userId=abc&test=a") },
		"duration":       func(i int) Value { return intValue(rand.Intn(1000) + 10) },
		"isError":        func(i int) Value { return boolValue([]byte{1, 0, 1, 0, 1}[i%5]) },
		"isLogin":        func(i int) Value { return boolValue([]byte{1, 0, 1, 0, 1}[i%5]) },
		"os":             func(i int) Value { return strValue([]string{"windows", "linux"}[i%2]) },
		"ip":             func(i int) Value { return strValue("192.168.3.1") },
		"serverIp":       func(i int) Value { return strValue("192.168.3.2") },
		"xpath":          func(i int) Value { return strValue("/div/table/tr/td") },
		"referer":        func(i int) Value { return strValue("http://www.sina.com.cn") },
	}
	frame := NewFrame(&ti, dims, metrics)
	var err error
	for i := 0; err == nil && i < 1000000; i++ { //214694
		err = frame.Add(func(name string) (value Value, err error) {
			return getter[name](i), nil
		}, func(iter func(key string) error) (err error) {
			if err = iter(ti.Name); err != nil {
				return
			}
			for i := range dims {
				if err = iter(dims[i].Name); err != nil {
					return
				}
			}
			for i := range metrics {
				if err = iter(metrics[i].Name); err != nil {
					return
				}
			}
			return
		})
	}
	if err == nil {
		if err := frame.Save("file://C:/work/test.tar.gz"); err != nil {
			t.Log(err)
		}
	} else {
		t.Log(err)
	}
}

func Test_NewID(t *testing.T) {
	t.Log(NewID())
}

func Test_loadDict(t *testing.T) {
	if frame, _, err := LoadFrame("file://C:/Users/why76/OneDrive/src/go/Betel/conf/dicts/persons.tar.gz", ""); err != nil {
		t.Log(err)
	} else if err = frame.Reindex(); err != nil {
		t.Log(err)
	} else {
		cb := frame.Locate(func(iter func(name string, value Value)) {
			iter("id", strValue("a1"))
		})
		if cb != nil {
			t.Log(cb("ggid").Str())
			t.Log(cb("name").Str())
		}

	}
}
