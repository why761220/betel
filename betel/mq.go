package betel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const MaxQueueSize = 10000

type Packet struct {
	Topic  string              `json:"topic,omitempty"`
	Header map[string][]string `json:"header,omitempty"`
	Size   int                 `json:"size,omitempty"`
	data   []byte
}
type Consumer struct {
	id     string
	gid    string
	grp    *Group
	topics map[string]struct{}
	ctx    context.Context
	cancel context.CancelFunc
	last   int64
}

type AddConsumerCmd struct {
	id     string
	gid    string
	topics map[string]struct{}
	cs     *Consumer
	chs    []Chans
	err    error
	done   sync.WaitGroup
}
type Chans struct {
	bad  chan Packet
	data chan Packet
}
type Group struct {
	mq     *MQ
	topics map[string]Chans
	cs     map[string]*Consumer
}

func (g Group) post(p Packet) {
	if ch, ok := g.topics[p.Topic]; ok {
		ch.data <- p
	}
}
func (g *Group) join(cmd *AddConsumerCmd) {
	if ts, ok := g.cs[cmd.id]; ok {
		ts.cancel()
		ts.grp = nil
	}

	chs := make([]Chans, 0, len(cmd.topics))
	for k, _ := range cmd.topics {
		ch, ok := g.topics[k]
		if !ok {
			ch = Chans{
				bad:  make(chan Packet, 100),
				data: make(chan Packet, 10000),
			}
			g.topics[k] = ch
		}
		chs = append(chs, ch)
	}
	cs := &Consumer{id: cmd.id, gid: cmd.gid, topics: cmd.topics, grp: g}
	cs.ctx, cs.cancel = context.WithCancel(cs.grp.mq.ctx)
	cmd.cs, cmd.chs, cmd.err = cs, chs, nil
	g.cs[cmd.id] = cs
	cs.grp.mq.exited.Add(1)
}

func (g *Group) leave(cs *Consumer) bool {
	if ts, ok := g.cs[cs.id]; ok {
		delete(g.cs, ts.id)
		ts.grp = nil
		return true
	} else {
		return false
	}
}

type MQ struct {
	Node
	ctx       context.Context
	cancel    context.CancelFunc
	exited    sync.WaitGroup
	groups    map[string]*Group
	chanData  chan Packet
	chanJoin  chan *AddConsumerCmd
	chanLeave chan *Consumer
}

func (this *MQ) post(data Packet) {
	for _, grp := range this.groups {
		grp.post(data)
	}
}
func (this *MQ) join(cmd *AddConsumerCmd) {
	defer cmd.done.Done()
	grp, ok := this.groups[cmd.gid]
	if !ok {
		grp = &Group{
			mq:     this,
			topics: make(map[string]Chans),
			cs:     make(map[string]*Consumer),
		}
		this.groups[cmd.gid] = grp
	}
	grp.join(cmd)
}

func (this *MQ) leave(cs *Consumer) {
	for _, grp := range this.groups {
		grp.leave(cs)
	}
}
func (this *MQ) mainLoop(exited *sync.WaitGroup) {
	exited.Add(1)
	defer exited.Done()
	srv := http.Server{Addr: ":" + strconv.FormatInt(int64(this.Port), 10), Handler: http.StripPrefix("/"+this.Preffix+"/msg", this)}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Println(err)
		}
	}()
	var etag string
	var err error
	if etag, err = this.Regiser(etag); err != nil {
		log.Println(err)
	}
	ti := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-this.ctx.Done():
			if err := srv.Shutdown(context.Background()); err != nil {
				log.Println(err)
			}
			this.cancel()
			this.exited.Wait()
			return
		case cs := <-this.chanLeave:
			this.leave(cs)
		case cmd := <-this.chanJoin:
			this.join(cmd)
		case data := <-this.chanData:
			this.post(data)
		case <-ti.C:
			if etag, err = this.Regiser(etag); err != nil {
				log.Println(err)
			}
		}
	}
}
func (this *MQ) Regiser(_etag string) (etag string, err error) {
	var bs []byte
	etag = _etag
	addr := this.Coord + "/watch/mq/" + this.ID
	if etag != "" {
		if req, err := http.NewRequest(http.MethodGet, addr, nil); err != nil {
			return etag, err
		} else {
			req.Header.Set("If-None-Match", etag)
			if resp, err := http.DefaultClient.Do(req); err != nil {
				return etag, err
			} else if bs, err = ioutil.ReadAll(resp.Body); err != nil {
				return etag, err
			} else if err = resp.Body.Close(); err != nil {
				return etag, err
			} else if resp.StatusCode == http.StatusNotModified {
				return etag, nil
			}
		}
	}
	if bs, err = json.Marshal(this); err != nil {
		return etag, err
	} else if resp, err := http.Post(addr, "application/x-www-form-urlencoded", bytes.NewReader(bs)); err != nil {
		return etag, err
	} else if bs, err = ioutil.ReadAll(resp.Body); err != nil {
		return etag, err
	} else if err = resp.Body.Close(); err != nil {
		return etag, err
	} else if resp.StatusCode != 200 {
		return etag, errors.New(fmt.Sprintf("http status code:%d error:%s", resp.StatusCode, string(bs)))
	} else {
		return resp.Header.Get("ETag"), nil
	}
}
func (this *MQ) AddConsumer(id, gid string, topics map[string]struct{}) (cs *Consumer, chs []Chans, err error) {
	cmd := AddConsumerCmd{id: id, gid: gid, topics: topics}
	cmd.done.Add(1)
	this.chanJoin <- &cmd
	cmd.done.Wait()
	cs, chs, err = cmd.cs, cmd.chs, cmd.err
	return
}

func (this *MQ) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	switch strings.ToUpper(request.Method) {
	case "GET":
		this.ConsumeHandler(writer, request)
	case "POST":
		this.ProduceHandler(writer, request)
	default:
		http.Error(writer, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}
func (this *MQ) ProduceHandler(writer http.ResponseWriter, request *http.Request) {
	topic := strings.TrimPrefix(request.URL.Path, "/")
	if bs, err := ioutil.ReadAll(request.Body); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	} else if header, err := url.ParseQuery(request.URL.RawQuery); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	} else {
		this.chanData <- Packet{Topic: topic, Header: header, data: bs}
	}
}
func (this *MQ) ConsumeHandler(writer http.ResponseWriter, request *http.Request) {
	var ok bool
	var p Packet
	var ch int
	var v reflect.Value
	var gid, id string
	var notify <-chan bool
	topics := map[string]struct{}{}
	flusher, ok := writer.(http.Flusher)
	if !ok {
		panic("expected http.ResponseWriter to be an http.Flusher")
	}

	if t, ok := writer.(http.CloseNotifier); !ok {
		notify = make(chan bool) //兼容而已！
	} else {
		notify = t.CloseNotify()
	}
	if values, err := url.ParseQuery(request.URL.RawQuery); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	} else {
		for _, t := range values["topics"] {
			topics[t] = struct{}{}
		}
		if tv := values["id"]; len(tv) >= 1 {
			id = tv[0]
		}
		if tv := values["gid"]; len(tv) >= 1 {
			gid = tv[0]
		}
	}
	if len(topics) == 0 || id == "" {
		http.Error(writer, "", http.StatusNotFound)
		return
	}
	if gid == "" {
		gid = id
	}
	writer.Header().Set("X-Content-Type-Options", "nosniff")

	cs, chs, err := this.AddConsumer(id, gid, topics)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	defer cs.grp.mq.exited.Done()
	chCount := len(chs)
	chsRecv := make([]reflect.SelectCase, 0, 2+chCount*2)
	chsRecv = append(chsRecv,
		reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(cs.ctx.Done()),
		}, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(notify),
		})

	for _, ch := range chs {
		chsRecv = append(chsRecv, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch.bad),
		})
	}
	for _, ch := range chs {
		chsRecv = append(chsRecv, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch.data),
		})
	}
	atomic.StoreInt64(&cs.last, time.Now().UnixNano())
	for {
		ch, v, ok = reflect.Select(chsRecv)
		if ch == 0 {
			return
		} else if ch == 1 || !ok {
			this.chanLeave <- cs
			return
		}
		ch -= 2
		if ch >= chCount {
			ch = ch - chCount
		}
		if p, ok = v.Interface().(Packet); ok {
			p.Size = len(p.data)
			header, err := json.Marshal(p)
			if err != nil {
				this.chanLeave <- cs
				chs[ch].bad <- p
				http.Error(writer, "", http.StatusInternalServerError)
				return
			}
			for _, bs := range [][]byte{header, []byte("\t"), p.data} {
				if _, err = writer.Write(bs); err != nil {
					this.chanLeave <- cs
					chs[ch].bad <- p
					log.Println(err)
					return
				}
			}
			flusher.Flush()
			atomic.StoreInt64(&cs.last, time.Now().UnixNano())
		}
	}
}

func StartMQ(ctx context.Context, exited *sync.WaitGroup, args StartArgs) (err error) {
	if args.Preffix == "" {
		args.Preffix = args.ID
	}
	mq := MQ{
		Node:      Node(args),
		chanData:  make(chan Packet, MaxQueueSize),
		chanJoin:  make(chan *AddConsumerCmd, 10),
		chanLeave: make(chan *Consumer, 10),
		groups:    make(map[string]*Group),
	}
	mq.ctx, mq.cancel = context.WithCancel(ctx)
	go mq.mainLoop(exited)
	return
}
