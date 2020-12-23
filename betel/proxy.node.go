package betel

import (
	"betel/errs"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Router struct {
	Host    string `json:"host,omitempty"`
	Port    int    `json:"port,omitempty"`
	URI     string `json:"uri,omitempty"`
	Mapping string `json:"mapping,omitempty"`
	IsQuery bool   `json:"isQuery,omitempty"`
	Failed  int64  `json:"failed,omitempty"`
	Succees int64  `json:"succees,omitempty"`
}
type mux struct {
	routes  map[string][]*Router
	queries map[string][]*Router
}

func (r Router) URL(u *url.URL) *url.URL {
	u.Scheme = "http"
	u.Host = r.Host + ":" + strconv.FormatInt(int64(r.Port), 10)
	u.Path = strings.Replace(u.Path, r.URI, r.Mapping, 1)
	return u
}
func (r Router) QueryUrl() string {
	return "http://" + r.Host + ":" + strconv.FormatInt(int64(r.Port), 10) + r.Mapping
}

type Proxy struct {
	Node
	mux *mux
}

func StartProxy(ctx context.Context, exited *sync.WaitGroup, args StartArgs) (err error) {

	var srv, tls *http.Server
	this := &Proxy{
		Node: Node(args),
		mux:  &(mux{}),
	}
	this.Type, this.Status = NtProxy, NsStart
	wg := sync.WaitGroup{}
	if args.Port > 0 {
		println("proxy server at:http://" + args.Host + ":" + strconv.FormatInt(int64(args.Port), 10))
		srv = &http.Server{Addr: ":" + strconv.FormatInt(int64(args.Port), 10), Handler: this}
		go func() {
			wg.Add(1)
			defer wg.Done()
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				errs.Println(err)
			}
		}()
	}
	if args.TLSPort > 0 {
		if _, err := os.Stat(args.CertFile); err == nil {
			if _, err = os.Stat(args.KeyFile); err == nil {
				println("proxy server at:https://" + args.Host + ":" + strconv.FormatInt(int64(args.TLSPort), 10))
				tls = &http.Server{Addr: ":" + strconv.FormatInt(int64(args.TLSPort), 10), Handler: this}
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := tls.ListenAndServeTLS(args.CertFile, args.KeyFile); err != http.ErrBodyNotAllowed {
						errs.Println(err)
					}
				}()
			}
		}
	}
	exited.Add(1)
	go func() {
		defer exited.Done()
		var err error
		etag := ""
		time.Sleep(time.Millisecond * 500)
		for {
			if err = Hookup(this.Coord, &this.Node, true); err != nil {
				errs.Println(err)
				time.Sleep(time.Second * 3)
			} else {
				break
			}
		}
		for {
			if etag, err = this.syncLoop(etag); err != nil {
				errs.Println(err)
				time.Sleep(time.Second * 3)
			} else {
				break
			}
		}
		ti := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-ctx.Done():
				if srv != nil {
					if err := srv.Shutdown(context.Background()); err != nil {
						errs.Println(err)
					}
				}
				if tls != nil {
					if err := tls.Shutdown(context.Background()); err != nil {
						errs.Println(err)
					}
				}
				wg.Wait()
				return
			case <-ti.C:
				if err = Hookup(this.Coord, &this.Node, false); err != nil {
					errs.Println(err)
				}
				if etag, err = this.syncLoop(etag); err != nil {
					errs.Println(err)
				}
			}
		}
	}()
	return
}

func (this *Proxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method == "OPTIONS" {
		writer.Header().Set("Access-Control-Allow-Origin", "*")
	} else if isQuery, routes := this.findRoutes(request.URL.Path); isQuery {
		if request.Method != "GET" && request.Method != "POST" {
			http.Error(writer, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		} else if err := this.doQuery(routes, writer, request); err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}
	} else {
		var complated bool
		for _, router := range routes {
			complated = false
			(&httputil.ReverseProxy{
				Director: func(req *http.Request) {
					req.URL = router.URL(req.URL)
				},
				ModifyResponse: func(resp *http.Response) error {
					this.Complated(router)
					complated = true
					return nil
				},
				ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
					if err == context.Canceled {
						complated = true
						return
					}
					this.BadRouter(router)
				},
			}).ServeHTTP(writer, request)
			if complated {
				return
			}
		}
		http.Error(writer, "", http.StatusBadGateway)
	}
}

func (*Proxy) Complated(ret *Router) {
	atomic.AddInt64(&ret.Succees, 1)
}
func (*Proxy) BadRouter(node *Router) {
	atomic.AddInt64(&node.Failed, 1)
}
func (this *Proxy) findRoutes(uri string) (isQuery bool, rs []*Router) {
	var (
		ts         []*Router
		k, preffix string
		ok         bool
	)
	mux := *(*mux)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.mux))))
	if rs, ok = mux.queries[uri]; ok {
		isQuery = true
		return
	}
	for k, ts = range mux.routes {
		if strings.HasPrefix(uri, k) {
			if preffix == "" || len(preffix) < len(k) {
				preffix, rs = k, ts
			}
		}
	}
	if len(rs) == 0 {
		return false, nil
	}
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].Failed == rs[j].Failed {
			return rs[i].Succees <= rs[j].Succees
		} else {
			return rs[i].Failed < rs[j].Failed
		}
	})
	return
}
func (this *Proxy) syncLoop(_etag string) (etag string, err error) {
	var bs []byte
	var rs []Router

	routes := make(map[string][]*Router)
	queries := make(map[string][]*Router)
	etag = _etag
	var resp *http.Response
	var req *http.Request
	if req, err = http.NewRequest("GET", this.Coord+"/nodes/routes", nil); err != nil {
		return
	}
	req.Header.Set("If-None-Match", etag)
	if resp, err = http.DefaultClient.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()
	if bs, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	if resp.StatusCode == http.StatusNotModified {
		return
	} else if resp.StatusCode != http.StatusOK {
		err = errs.New(string(bs))
		return
	}
	etag = resp.Header.Get("ETag")

	if err = json.Unmarshal(bs, &rs); err != nil {
		return
	}
	for k := range rs {
		r := rs[k]
		if r.IsQuery {
			queries[r.URI] = append(queries[r.URI], &r)
		} else {
			routes[r.URI] = append(routes[r.URI], &r)
		}
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&this.mux)), unsafe.Pointer(&mux{routes: routes, queries: queries}))
	return
}
