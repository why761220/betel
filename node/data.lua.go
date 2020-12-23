package node

import (
	mut "betel/vars"
	lua "github.com/yuin/gopher-lua"
	"math"
	"sync"
)

var LuaCtxPool = sync.Pool{
	New: func() interface{} {
		return NewLuaCtx()
	},
}

type LuaContext struct {
	L    *lua.LState
	data *mut.DataFrame
	pos  int
}

func NewLuaCtx() *LuaContext {
	this := &LuaContext{L: lua.NewState()}
	this.L.SetGlobal("Get", this.L.NewFunction(this.get))
	this.L.SetGlobal("Set", this.L.NewFunction(this.set))
	return this
}
func (this *LuaContext) Reset(data *mut.DataFrame) {
	this.data, this.pos = data, -1
}
func (this *LuaContext) Next() (ret bool) {
	if ret = this.pos < this.data.Count(); ret {
		this.pos++
	}
	return
}
func (this *LuaContext) set(L *lua.LState) int {
	name, v := L.ToString(1), L.Get(2)
	switch v.Type() {
	case lua.LTNil:
		this.data.Set(this.pos, name, nil)
		L.Push(lua.LBool(true))
	case lua.LTBool:
		value, _ := mut.NewValue(bool(v.(lua.LBool)))
		this.data.Set(this.pos, name, value)
		L.Push(lua.LBool(true))
	case lua.LTNumber:
		if i, d := math.Modf(float64(v.(lua.LNumber))); d == 0 {
			value, _ := mut.NewValue(int64(i))
			this.data.Set(this.pos, name, value)
			L.Push(lua.LBool(true))
		} else {
			value, _ := mut.NewValue(float64(v.(lua.LNumber)))
			this.data.Set(this.pos, name, value)
			L.Push(lua.LBool(true))
		}
	case lua.LTString:
		value, _ := mut.NewValue(string(v.(lua.LString)))
		this.data.Set(this.pos, name, value)
		L.Push(lua.LBool(true))
	default:
		L.Push(lua.LBool(false))
	}
	return 1
}
func (this *LuaContext) get(L *lua.LState) int {
	if v := this.data.Get(this.pos, L.ToString(1)); v == nil {
		L.Push(lua.LNil)
	} else {
		switch v.Type() & mut.TypeMask {
		case mut.TypeBool:
			L.Push(lua.LBool(v.Bool()))
		case mut.TypeInt, mut.TypeTime:
			L.Push(lua.LNumber(v.Int()))
		case mut.TypeFloat:
			L.Push(lua.LNumber(v.Float()))
		case mut.TypeStr:
			L.Push(lua.LString(v.Str()))
		default:
			L.Push(lua.LNil)
		}
	}
	return 1
}
