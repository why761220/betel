package betel

import (
	"betel/errs"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Coord struct {
	Node
	lock    sync.RWMutex
	Points  WatchPoint
	Values  map[string]WatchPoint
	chanAdd chan Node
	nodes   *map[string]*Node
	routes  *Routes
}
type Routes struct {
	routes []Router
	etag   string
}
type Subscriber struct {
	Path string
	path []string
	Url  string
}
type AddPoint struct {
	WatchPoint
	Path string
}
type WatchPointStatus int

const (
	WpsUnkown WatchPointStatus = iota
	WpsReplace
	WpsAdd
	WpsExpires
)

type Feedback struct {
	WatchPoint
	Status WatchPointStatus
	err    error
	dst    string
}
type WatchPoint struct {
	Value    interface{}            `json:"value,omitempty"`
	ErrCount int64                  `json:"errCount,omitempty"`
	ETag     int64                  `json:"etag,omitempty"`
	Points   map[string]*WatchPoint `json:"points,omitempty"`
	expires  time.Time
	Owner    string
}

var (
	etagLock  = sync.Mutex{}
	etagCount = int64(0)
)

func NewETag() (ret int64) {
	etagLock.Lock()
	etagCount++
	ret = etagCount
	etagLock.Unlock()
	return
}
func (p *WatchPoint) Find(path []string) *WatchPoint {
	var (
		ok  bool
		ret = p
		key string
	)
	for {
		if len(path) == 0 {
			return ret
		}
		key, path = path[0], path[1:]
		if ret, ok = ret.Points[key]; !ok {
			return nil
		}
	}
}
func (p WatchPoint) Clone(etag int64) (interface{}, int64) {
	if len(p.Points) == 0 {
		if p.ETag <= etag {
			return nil, etag
		} else {
			return p.Value, p.ETag
		}
	}
	max := etag
	var ret map[string]interface{}
	for k, s := range p.Points {
		if child, t := s.Clone(etag); child != nil {
			if ret == nil {
				ret = make(map[string]interface{})
			}
			ret[k] = child
			if t > max {
				max = t
			}
		}
	}
	if ret == nil {
		return nil, etag
	} else {
		return ret, max
	}
}

func (p *WatchPoint) Add(path []string, value interface{}) int64 {
	var (
		ok      bool
		key     string
		t       *WatchPoint
		target  = p
		etag    = NewETag()
		parents = make([]*WatchPoint, 0, 10)
	)

	for {
		if len(path) == 0 {
			target.Value = value
			target.ETag = etag
			for i := range parents {
				parents[i].Value = nil
				parents[i].ETag = etag
			}
			return etag
		}
		key, path = path[0], path[1:]
		if target.Points == nil {
			target.Points = make(map[string]*WatchPoint)
			t = &WatchPoint{ETag: etag}
			target.Points[key] = t
		} else if t, ok = target.Points[key]; !ok {
			t = &WatchPoint{ETag: etag}
			target.Points[key] = t
		}
		parents = append(parents, target)
		target = t
	}
}
func (p *WatchPoint) delete(path []string) {
	var (
		ok     bool
		key    string
		s      *WatchPoint
		parent = p
	)
	if len(path) == 0 {
		return
	}
	for {
		key, path = path[0], path[1:]
		if s, ok = parent.Points[key]; !ok {
			return
		} else if len(path) == 0 {
			delete(parent.Points, key)
			return
		} else {
			parent = s
		}
	}
}

func (proxy *Coord) GetWatchPoint(path []string, eTag int64, w http.ResponseWriter) {
	proxy.lock.RLock()
	defer proxy.lock.RUnlock()
	if p := proxy.Points.Find(path); p == nil {
		http.NotFound(w, nil)
	} else if r, etag := p.Clone(eTag); r == nil {
		http.Error(w, "", http.StatusNotModified)
	} else {
		w.Header().Set("ETag", strconv.FormatInt(etag, 10))
		if t, ok := r.(map[string]interface{}); ok {
			if bs, err := json.Marshal(t); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			} else if _, err = w.Write(bs); err != nil {
				errs.Println(err)
			}
		} else if bs, ok := r.([]byte); ok {
			if _, err := w.Write(bs); err != nil {
				errs.Println(err)
			}
		} else if s, ok := r.(string); ok {
			if _, err := w.Write([]byte(s)); err != nil {
				errs.Println(err)
			}
		} else if i, ok := r.(int64); ok {
			if _, err := w.Write([]byte(strconv.FormatInt(i, 10))); err != nil {
				errs.Println(err)
			}
		} else if f, ok := r.(float64); ok {
			if _, err := w.Write([]byte(strconv.FormatFloat(f, 'f', -1, 64))); err != nil {
				errs.Println(err)
			}
		} else if b, ok := r.(bool); ok {
			if _, err := w.Write([]byte(strconv.FormatBool(b))); err != nil {
				errs.Println(err)
			}
		} else {
			http.Error(w, "type is error!", http.StatusInternalServerError)
		}
	}
}
func (proxy *Coord) DelWatchPoint(path []string) {
	proxy.lock.Lock()
	defer proxy.lock.Unlock()
	proxy.Points.delete(path)
}

func (proxy *Coord) PostWatchPoint(path []string, data interface{}) int64 {
	proxy.lock.Lock()
	defer proxy.lock.Unlock()
	return proxy.Points.Add(path, data)
}
func (proxy *Coord) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	var err error
	var path []string
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	if request.URL.Path != "" {
		path = strings.Split(request.URL.Path, "/")
	}
	switch strings.ToUpper(request.Method) {
	case "GET":
		eTag := int64(-1)
		if t := request.Header.Get("If-None-Match"); t != "" {
			if eTag, err = strconv.ParseInt(t, 10, 64); err != nil {
				errs.Println(err)
			}
		}
		proxy.GetWatchPoint(path, eTag, writer)
	case "POST":
		if bs, err := ioutil.ReadAll(request.Body); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			errs.Println(err)
		} else {
			var etag int64
			switch request.Header.Get("Content-Type") {
			case "application/json":
				var v interface{}
				if err := json.Unmarshal(bs, &v); err == nil {
					etag = proxy.PostWatchPoint(path, v)
				} else {
					etag = proxy.PostWatchPoint(path, bs)
				}
			case "text/plain":
				s := string(bs)
				if b, err := strconv.ParseBool(s); err == nil {
					etag = proxy.PostWatchPoint(path, b)
				} else if i, err := strconv.ParseInt(s, 10, 64); err == nil {
					etag = proxy.PostWatchPoint(path, i)
				} else if f, err := strconv.ParseFloat(s, 64); err == nil {
					etag = proxy.PostWatchPoint(path, f)
				} else {
					etag = proxy.PostWatchPoint(path, s)
				}
			default:
				etag = proxy.PostWatchPoint(path, bs)
			}
			writer.Header().Set("ETag", strconv.FormatInt(etag, 10))
		}
	case "DELETE":
		proxy.DelWatchPoint(path)
	}
}

func StartCoord(ctx context.Context, exited *sync.WaitGroup, args StartArgs) (err error) {
	var srv, tls *http.Server
	wg := sync.WaitGroup{}
	coord := &Coord{
		Node:    Node(args),
		chanAdd: make(chan Node, 1000),
		nodes:   &(map[string]*Node{}),
		routes:  &(Routes{}),
		Points:  WatchPoint{ETag: 0, Points: make(map[string]*WatchPoint)},
	}
	if args.Port > 0 {
		mux := http.NewServeMux()
		pattern := "/" + coord.ID + "/nodes/add/"
		mux.Handle(pattern, http.StripPrefix(pattern, FuncHandler{f: coord.webAddNodes}))
		pattern = "/" + coord.ID + "/nodes/list"
		mux.HandleFunc(pattern, coord.webGetNodes)
		pattern = "/" + coord.ID + "/nodes/routes"
		mux.HandleFunc(pattern, coord.webGetRoutes)
		pattern = "/" + coord.ID + "/nodes/proxy/list"
		mux.HandleFunc(pattern, coord.webProxyGet)

		pattern = "/" + coord.ID + "/conf/" + DictsName + "/"
		mux.Handle(pattern, http.StripPrefix(pattern, FileServer{root: path.Join(coord.ConfDir, DictsName), Post: DictPost}))
		pattern = "/" + coord.ID + "/conf/" + DescsName + "/"
		mux.Handle(pattern, http.StripPrefix(pattern, FileServer{root: path.Join(coord.ConfDir, DescsName), Post: DescPost}))
		pattern = "/" + coord.ID + "/conf/" + FlowsName + "/"
		mux.Handle(pattern, http.StripPrefix(pattern, FileServer{root: path.Join(coord.ConfDir, FlowsName), Post: FlowPost}))
		pattern = "/" + coord.ID + "/conf/" + PythonName + "/"
		mux.Handle(pattern, http.StripPrefix(pattern, FileServer{root: path.Join(coord.ConfDir, PythonName), Post: FlowPost}))
		pattern = "/" + coord.ID + "/conf/whl/"
		mux.Handle(pattern, http.StripPrefix(pattern, FileServer{root: path.Join(coord.ConfDir, "whl")}))
		pattern = "/" + coord.ID + "/identify/get"
		mux.HandleFunc(pattern, coord.webGetIdentify)
		coord.AddMux("/"+coord.Preffix+"/identify/get", pattern, false)
		pattern = "/" + coord.ID + "/token/get"
		mux.HandleFunc(pattern, coord.webTokenGet)
		coord.AddMux("/"+coord.Preffix+"/token/get", pattern, false)
		println("coord server at:http://" + args.Host + ":" + strconv.FormatInt(int64(args.Port), 10) + "/" + args.Preffix)
		srv = &http.Server{Addr: ":" + strconv.FormatInt(int64(args.Port), 10), Handler: mux}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				errs.Println(err)
			}
		}()
	}
	if args.TLSPort > 0 {
		if _, err := os.Stat(args.CertFile); err == nil {
			if _, err = os.Stat(args.KeyFile); err == nil {
				mux := http.NewServeMux()
				pattern := "/" + coord.Preffix + "/watch/"
				mux.Handle(pattern, http.StripPrefix(pattern, coord))
				pattern = "/" + coord.Preffix + "/conf/" + DictsName + "/"
				mux.Handle(pattern, http.StripPrefix(pattern, FileServer{root: path.Join(coord.ConfDir, DictsName), Post: DictPost}))
				pattern = "/" + coord.Preffix + "/conf/" + DescsName + "/"
				mux.Handle(pattern, http.StripPrefix(pattern, FileServer{root: path.Join(coord.ConfDir, DescsName), Post: DescPost}))
				pattern = "/" + coord.Preffix + "/conf/" + FlowsName + "/"
				mux.Handle(pattern, http.StripPrefix(pattern, FileServer{root: path.Join(coord.ConfDir, FlowsName), Post: FlowPost}))
				pattern = "/" + coord.Preffix + "/conf/" + PythonName + "/"
				mux.Handle(pattern, http.StripPrefix(pattern, FileServer{root: path.Join(coord.ConfDir, PythonName), Post: FlowPost}))
				mux.HandleFunc("/"+coord.Preffix+"/token/get", coord.webTokenGet)
				mux.HandleFunc("/"+coord.Preffix+"/user/add", coord.webUserAdd)
				println("coord server at:https://" + args.Host + ":" + strconv.FormatInt(int64(args.TLSPort), 10) + "/" + args.Preffix)
				tls = &http.Server{Addr: ":" + strconv.FormatInt(int64(args.TLSPort), 10), Handler: mux}
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
		ticker := time.NewTicker(time.Minute)
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
			case node := <-coord.chanAdd:
				coord.nodeAdd(&node)
			case <-ticker.C:
				coord.updateRoutes(false)
			}
		}
	}()
	return
}

func (this *Coord) nodeAdd(node *Node) {
	nodes := make(map[string]*Node)
	node.expires = time.Now().Add(time.Minute).UnixNano()
	nodes[node.ID] = node
	olds := *(*map[string]*Node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.nodes))))
	for id, old := range olds {
		if _, ok := nodes[id]; !ok {
			nodes[id] = old
		}
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&this.nodes)), unsafe.Pointer(&nodes))
	this.updateRoutes(true)
}
func (this *Coord) updateRoutes(force bool) {
	ti := time.Now().UnixNano()
	var routes []Router
	var expires map[string]struct{}
	nodes := *(*map[string]*Node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.nodes))))
	for id, node := range nodes {
		if atomic.LoadInt64(&node.expires) < ti {
			if expires == nil {
				expires = make(map[string]struct{})
			}
			expires[id] = struct{}{}
		} else {
			if node.Mapping {
				r := Router{}
				r.URI, r.Mapping, r.Host, r.Port = "/"+node.ID, "/"+node.ID, node.Host, node.Port
				routes = append(routes, r)
			}
			for uri, dst := range node.Mux {
				r := Router{}
				r.URI, r.Mapping, r.Host, r.Port, r.IsQuery = uri, dst.Mapping, node.Host, node.Port, dst.IsQuery
				routes = append(routes, r)
			}
		}
	}
	if len(expires) > 0 {
		news := make(map[string]*Node)
		for id, node := range nodes {
			if _, ok := expires[id]; !ok {
				news[id] = node
			}
		}
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&this.nodes)), unsafe.Pointer(&news))
	}
	if len(expires) > 0 || force {
		for uri, dst := range this.Mux {
			r := Router{}
			r.URI, r.Mapping, r.Host, r.Port, r.IsQuery = uri, dst.Mapping, this.Host, this.Port, dst.IsQuery
			routes = append(routes, r)
		}
		etag := strings.ReplaceAll(uuid.New().String(), "-", "")
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&this.routes)), unsafe.Pointer(&Routes{routes: routes, etag: etag}))
	}
}

func (this *Coord) webGetRoutes(w http.ResponseWriter, r *http.Request) {
	routes := *(*Routes)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.routes))))
	if t := r.Header.Get("If-None-Match"); t != "" {
		if t == routes.etag {
			http.Error(w, "", http.StatusNotModified)
			return
		}
	}
	w.Header().Set("ETag", routes.etag)
	if bs, err := json.Marshal(routes.routes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if _, err = w.Write(bs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (this *Coord) webGetNodes(w http.ResponseWriter, r *http.Request) {
	routes := *(*map[string]*Node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.nodes))))
	if bs, err := json.Marshal(routes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if _, err = w.Write(bs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (this *Coord) webAddNodes(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path
	var node Node
	ti := time.Now().UnixNano()
	switch strings.ToUpper(r.Method) {
	case "GET":
		nodes := *(*map[string]*Node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.nodes))))
		if node, ok := nodes[id]; ok {
			old := atomic.SwapInt64(&node.expires, time.Now().Add(time.Minute).UnixNano())
			if old < ti {
				this.updateRoutes(false)
			}
		} else {
			http.NotFound(w, r)
		}
	case "PUT", "POST":
		if bs, err := ioutil.ReadAll(r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else if err = json.Unmarshal(bs, &node); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			this.chanAdd <- node
		}
	default:
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}
