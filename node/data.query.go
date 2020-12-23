package node

import (
	"betel/errs"
	mut "betel/vars"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

type nodeQuery struct {
	*dataNode
}

type QueryArgs struct {
	Name   string           `json:"name,omitempty"`
	Where  string           `json:"where,omitempty"`
	Having string           `json:"having,omitempty"`
	Start  string           `json:"start,omitempty"`
	End    string           `json:"end,omitempty"`
	Time   *mut.Column      `json:"Column,omitempty"`
	Grps   []mut.Column     `json:"grps,omitempty"`
	Aggs   []mut.Column     `json:"aggs,omitempty"`
	Outs   []string         `json:"outs,omitempty"`
	Sorts  []mut.Sort       `json:"sorts,omitempty"`
	Begin  int              `json:"begin,omitempty"`
	Limit  int              `json:"limit,omitempty"`
	Format mut.PacketFormat `json:"format,omitempty"`
	Layout string           `json:"layout,omitempty"`
	Prec   string           `json:"prec,omitempty"`
	Delim  string           `json:"delim,omitempty"`
}

func (args QueryArgs) GetDelim() string {
	if args.Delim == "" {
		return ","
	} else {
		return args.Delim
	}
}
func (args QueryArgs) ProxyArgs() (dst *mut.DataFrame, having *mut.Expr, fmt mut.PacketFormat, err error) {
	if strings.TrimSpace(args.Name) == "" {
		err = errs.New("query name is empty!")
		return
	}
	fmt = args.Format
	if args.Having != "" {
		if having, err = mut.ParseExpr(args.Having); err != nil {
			return
		}
	}
	if l := len(args.Aggs) + len(args.Grps); l > 0 {
		var ti *mut.Column
		var dims, metrics []mut.Column
		if args.Time != nil {
			t := *args.Time
			t.Field = ""
			ti = &t
		}
		if l = len(args.Grps); l > 0 {
			dims = make([]mut.Column, l)
			for i, col := range args.Grps {
				dims[i] = col
				dims[i].Field = ""
			}
		}
		if l = len(args.Aggs); l > 0 {
			metrics = make([]mut.Column, l)
			for i, col := range args.Aggs {
				metrics[i] = col
				metrics[i].Field = ""
			}
		}
		dst = mut.NewFrame(ti, dims, metrics)
	} else if len(args.Sorts) > 0 || having != nil || (args.Begin > 0 || args.Limit > 0) {
		dst = mut.AnyFrame()
	}
	return
}
func (args QueryArgs) NodeArgs(desc *mut.Desc) (dst *mut.DataFrame, where *mut.Expr, start, end int64, err error) {
	if strings.TrimSpace(args.Name) == "" {
		err = errs.New("query name is empty!")
		return
	}
	if args.Where != "" {
		if where, err = mut.ParseExpr(args.Where); err != nil {
			return
		}
	}
	if ti, err := mut.ParseTime(args.Start, args.Layout); err == nil {
		start = ti.UnixNano()
	}
	if ti, err := mut.ParseTime(args.End, args.Layout); err == nil {
		end = ti.UnixNano()
	}
	if count := len(args.Grps) + len(args.Aggs); count > 0 {
		dst = mut.NewFrame(args.Time, args.Grps, args.Aggs)
	} else if desc != nil {
		var dims, metrics []mut.Column
		var ti *mut.Column
		if ti = desc.Time; ti != nil {
			t := *desc.Time
			ti = &t
			ti.Field = ""
		}
		for i := range desc.Dimensions {
			dims = append(dims, desc.Dimensions[i])
			dims[i].Field = ""
		}
		for i := range desc.Metrics {
			metrics = append(metrics, desc.Metrics[i])
			metrics[i].Field = ""
		}
		dst = mut.NewFrame(ti, dims, metrics)
	} else if len(args.Sorts) > 0 && (args.Begin > 0 || args.Limit > 0) {
		dst = mut.AnyFrame()
	}
	return
}

type QueryCmd struct {
	QueryArgs
	Where *mut.Expr
	Start int64
	End   int64
	wg    sync.WaitGroup
	lock  sync.Mutex
	out   http.ResponseWriter
	dst   *mut.DataFrame
	err   *error
}

func (this *QueryCmd) SelectArgs(desc *mut.Desc) (e *mut.Expr, start, end int64, sorts []mut.Sort, limit1, limit2 int) {
	if this.Where != nil {
		e = this.Where
	}
	start, end = this.Start, this.End
	if len(this.Aggs) == 0 && len(this.Grps) == 0 && (desc == nil || len(desc.Dimensions) == 0) {
		sorts, limit1, limit2 = this.Sorts, this.Begin, this.Limit
	}
	return
}
func (this *QueryCmd) Reset(args QueryArgs, desc *mut.Desc) (ret *QueryCmd, err error) {
	this.QueryArgs = args
	this.dst, this.Where, this.Start, this.End, err = args.NodeArgs(desc)
	return this, err
}
func (this *QueryCmd) Close() {
	if this.dst != nil {
		this.dst.Close()
	}
}
func (this *QueryCmd) SetErr(err error) {
	if err != nil {
		atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&this.err)), nil, unsafe.Pointer(&err))
	}
}
func (this *QueryCmd) Stopped() bool {
	return atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.err))) != nil
}
func (this *QueryCmd) Write(src *mut.DataFrame, rows mut.Positions) (err error) {
	if rows.Len() <= 0 {
		return nil
	} else if this.dst != nil {
		this.lock.Lock()
		err = mut.Merge(this.dst, src, rows, nil)
		this.lock.Unlock()
	} else if this.out != nil {
		this.lock.Lock()
		if this.Format == mut.PfTxtRow {
			this.out.Header().Set("cols", strings.Join(src.GetNames(), this.GetDelim()))
		}
		fr := mut.FrameReader{DataFrame: src, Rows: rows, Format: this.Format, Layout: this.Layout, Prec: this.Prec, Delim: this.Delim}
		_, err = io.Copy(this.out, fr)
		this.lock.Unlock()
	} else {
		return errs.Logic()
	}
	return
}

func (this nodeQuery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var args QueryArgs
	var code int
	if r.Method != "POST" {
		http.Error(w, "", http.StatusMethodNotAllowed)
	} else if bs, err := ioutil.ReadAll(r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if err = json.Unmarshal(bs, &args); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if code, err = this.doQuery(args, w); err != nil {
		http.Error(w, err.Error(), code)
	}
}

func (this nodeQuery) doQuery(args QueryArgs, w http.ResponseWriter) (int, error) {
	if u, err := this.GetUint(args.Name, false); err != nil {
		return http.StatusInternalServerError, err
	} else if u == nil {
		return http.StatusNotFound, errs.New(http.StatusText(http.StatusNotFound)) //QueryArgs{Format: args.Format, Delim: args.Delim}
	} else if cmd, err := (&QueryCmd{QueryArgs: args, out: w}).Reset(args, u.GetDesc()); err != nil {
		return http.StatusInternalServerError, err
	} else {
		u.Query(cmd)
		cmd.wg.Wait()
		if r := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&cmd.err))); r != nil {
			return http.StatusInternalServerError, *(*error)(r)
		} else if cmd.dst != nil {
			defer cmd.dst.Close()
			var rows mut.Positions
			w.Header().Set("direct", "agg")
			if cmd.dst.IsDetail() && len(cmd.Sorts) > 0 && (cmd.Begin > 0 || cmd.Limit > 0) {
				if rows, err = cmd.dst.Sort(mut.NewOrderPositions(cmd.dst.Count()), cmd.Sorts, cmd.Begin, cmd.Limit); err != nil {
					return http.StatusInternalServerError, err
				}
			} else {
				rows = mut.AllPositions(cmd.dst.Count())
			}
			if _, err = io.Copy(w, mut.FrameReader{DataFrame: cmd.dst, Format: mut.PfTar, Rows: rows}); err != nil {
				return http.StatusInternalServerError, err
			}
		}
		return http.StatusOK, nil
	}
}
