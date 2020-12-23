package node

import (
	"betel/errs"
	mut "betel/vars"
	"encoding/base64"
	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
	"strings"
	"sync"
)

type Lookup struct {
	Ins  map[string]string `json:"ins,omitempty"`
	Outs map[string]string `json:"outs,omitempty"`
}
type GetDictCmd struct {
	wg      sync.WaitGroup
	dict    *mut.DataFrame
	name    string
	indices []string
	err     error
}
type Dict struct {
	*mut.DataFrame
	etag string
}

type Flow struct {
	Desc    *mut.Desc         `json:"desc,omitempty"`
	Paths   []string          `json:"paths,omitempty"`
	Scripts map[string]string `json:"scripts,omitempty"`
	Lookups map[string]Lookup `json:"lookups,omitempty"`
	Units   map[string]string `json:"units,omitempty"`
	scripts []*lua.FunctionProto
	units   map[string]*lua.FunctionProto
	etag    string
}
type GetFlowerCmd struct {
	wg   sync.WaitGroup
	name string
	flow *Flow
	err  error
}
type RecvCmd struct {
	wg    *sync.WaitGroup
	name  string
	data  *mut.DataFrame
	uints map[string]*lua.FunctionProto
	err   error
}
type LoadWorkCmd struct {
	uint *DataUnit
	info mut.FrameInfo
	data *mut.DataFrame
	err  error
}

func (this *Flow) init(etag string) (*Flow, error) {

	this.etag = etag
	reader := strings.NewReader("")
	for name, code := range this.Scripts {
		if code == "" {
			continue
		}
		if t, err := base64.StdEncoding.DecodeString(code); err == nil {
			reader.Reset(string(t))
		} else {
			reader.Reset(code)
		}
		if chunk, err := parse.Parse(reader, name); err != nil {
			return this, err
		} else if proto, err := lua.Compile(chunk, name); err != nil {
			return this, err
		} else {
			this.scripts = append(this.scripts, proto)
		}
	}
	for name, code := range this.Units {
		if this.units == nil {
			this.units = make(map[string]*lua.FunctionProto)
		}
		if code == "" {
			this.units[name] = nil
			continue
		}
		if t, err := base64.StdEncoding.DecodeString(code); err == nil {
			reader.Reset(string(t))
		} else {
			reader.Reset(code)
		}
		if chunk, err := parse.Parse(reader, name); err != nil {
			return this, err
		} else if proto, err := lua.Compile(chunk, name); err != nil {
			return this, err
		} else {
			this.units[name] = proto
		}
	}
	return this, nil
}

func (this *Flow) Verify() error {
	return nil
}

func (this *RecvCmd) doFlow(node *dataNode, flow *Flow) {
	if len(flow.units) > 0 {
		this.uints = make(map[string]*lua.FunctionProto)
		for k, v := range flow.units {
			this.uints[k] = v
		}
	} else {
		this.data.Close()
		this.completed(nil)
		return
	}
	for name, lk := range flow.Lookups {
		if dict, err := node.GetDict(name); err != nil {
			this.data.Close()
			if this.completed(err) {
				errs.Println(err)
			}
			return
		} else if dict != nil {
			for i, l := 0, this.data.Count(); i < l; i++ {
				if outs := dict.Locate(func(iter func(name string, value mut.Value)) {
					for src, dst := range lk.Ins {
						iter(dst, this.data.Get(i, src))
					}
				}); outs != nil {
					for dst, src := range lk.Outs {
						_, _ = this.data.Set(i, src, outs(dst))
					}
				}
			}
		}
	}
	if len(flow.scripts) > 0 {
		l := LuaCtxPool.Get().(*LuaContext)
		defer LuaCtxPool.Put(l)
		funcs := make([]*lua.LFunction, 0, len(flow.scripts))
		for i := range flow.scripts {
			funcs = append(funcs, l.L.NewFunctionFromProto(flow.scripts[i]))
		}
		for l.Reset(this.data); l.Next(); {
			for i := range funcs {
				l.L.Push(funcs[i])
				if err := l.L.PCall(0, lua.MultRet, nil); err != nil {
					this.data.Close()
					if this.completed(err) {
						errs.Println(err)
					}
					return
				}
			}
		}
	}
	this.next(node)
}
func (this *RecvCmd) next(node *dataNode) {
	for name, _ := range this.uints {
		delete(this.uints, name)
		if unit, err := node.GetUint(name, true); err != nil {
			this.completed(err)
			return
		} else {
			unit.chanData <- this
		}
		return
	}
	this.data.Close()
	this.completed(nil)
}

func (this *RecvCmd) completed(err error) (ret bool) {
	if ret = this.wg == nil; !ret {
		this.err = err
		this.wg.Done()
	}
	return
}
