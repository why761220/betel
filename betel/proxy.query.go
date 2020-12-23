package betel

import (
	"betel/errs"
	mut "betel/vars"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

func (this Proxy) doQuery(routes []*Router, w http.ResponseWriter, r *http.Request) (err error) {
	var bs []byte
	var e error
	var wg sync.WaitGroup
	var lock sync.Mutex
	var fmt mut.PacketFormat
	var dst *mut.DataFrame
	var args QueryArgs
	var rows mut.OrderPositions
	var having *mut.Expr
	var reprocess bool
	var first int32
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method == "POST" {
		if bs, err = ioutil.ReadAll(r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else if err = json.Unmarshal(bs, &args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		args.Name = r.URL.Query().Get("name")
		args.Where = r.URL.Query().Get("where")
		args.Layout = r.URL.Query().Get("layout")
		args.Format = mut.ParsePacketFormat(r.URL.Query().Get("format"), mut.PfTxtRow)
		args.Delim = r.URL.Query().Get("delim")
		args.Prec = r.URL.Query().Get("prec")
		args.Start = r.URL.Query().Get("start")
		args.End = r.URL.Query().Get("end")
	}
	if dst, having, fmt, err = args.ProxyArgs(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, route := range routes {
		wg.Add(1)
		this.queryWorker(&wg, route, args, &lock, &dst, w, &first, reprocess, &e)
	}
	wg.Wait()
	if err = errs.GetFrom(&e); err != nil {
		return err
	}
	if dst != nil {
		defer dst.Close()
		if having != nil {
			var p mut.Positions
			if p, err = dst.Select(having, 0, 0, args.Sorts, args.Begin, args.Limit); err != nil {
				return
			} else {
				rows = p.To(rows)
			}
		}
		if len(args.Sorts) > 0 {
			if rows == nil {
				rows = mut.NewOrderPositions(dst.Count())
			}
			if rows, err = dst.Sort(rows, args.Sorts, args.Begin, args.Limit); err != nil {
				return
			}
		}
		if fmt == mut.PfTxtRow {
			if _, err = w.Write([]byte(strings.Join(dst.GetNames(), args.Delim))); err != nil {
				return
			} else if _, err = w.Write([]byte{'\n'}); err != nil {
				return
			}

		}
		fr := mut.FrameReader{DataFrame: dst, Format: fmt, Rows: rows, Layout: args.Layout, Prec: args.Prec, Delim: args.Delim, Start: args.Begin, End: args.Limit}
		if _, err = io.Copy(w, fr); err != nil {
			return
		}

	}
	return
}

func (this Proxy) queryWorker(wg *sync.WaitGroup, route *Router, args QueryArgs, lock *sync.Mutex, dst **mut.DataFrame, w io.Writer, pFirst *int32, reprocess bool, e *error) {
	defer wg.Done()
	const contentType = "application/json"
	addr := route.QueryUrl()
	if bs, err := json.Marshal(args); err != nil {
		errs.SetTo(e, err)
	} else if resp, err := http.Post(addr, contentType, bytes.NewBuffer(bs)); err != nil {
		errs.SetTo(e, err)
	} else if resp.StatusCode == http.StatusOK {
		atomic.AddInt64(&route.Succees, 1)
		if resp.Header.Get("direct") == "agg" || reprocess {
			src := mut.AnyFrame()
			if _, err = io.Copy(&mut.FrameWriter{DataFrame: src}, resp.Body); err == nil {
				lock.Lock()
				if *dst == nil {
					*dst = src
				} else {
					errs.SetTo(e, mut.Merge(*dst, src, nil, nil))
					src.Close()
				}
				lock.Unlock()
			} else {
				errs.SetTo(e, err)
			}
		} else {
			first := atomic.CompareAndSwapInt32(pFirst, 0, 1)
			lock.Lock()
			if first {
				if _, err = w.Write([]byte(resp.Header.Get("cols"))); err == nil {
					if _, err = w.Write([]byte{'\n'}); err == nil {
						_, err = io.Copy(w, resp.Body)
					}
				}
			}
			lock.Unlock()
			errs.SetTo(e, err)
		}
		if err = resp.Body.Close(); err != nil {
			errs.Println(err)
		}
	} else if bs, err := ioutil.ReadAll(resp.Body); err != nil {
		atomic.AddInt64(&route.Failed, 1)
		errs.SetTo(e, err)
	} else if err = resp.Body.Close(); err != nil {
		atomic.AddInt64(&route.Failed, 1)
		errs.SetTo(e, err)
	} else {
		atomic.AddInt64(&route.Failed, 1)
		errs.SetTo(e, errors.New(string(bs)))
	}
}
