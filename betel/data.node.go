package betel

import (
	"betel/errs"
	mut "betel/vars"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type dataNode struct {
	Node
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	uints       sync.Map
	flows       sync.Map
	dicts       sync.Map
	chanData    chan *RecvCmd
	chanGetFlow chan *GetFlowerCmd
	chanGetDict chan *GetDictCmd
	chanBackup  chan BackupCmd
}

func StartData(ctx context.Context, exited *sync.WaitGroup, args StartArgs) error {
	var err error
	this := &dataNode{
		Node:        Node(args),
		ctx:         ctx,
		chanData:    make(chan *RecvCmd, 10000),
		chanGetFlow: make(chan *GetFlowerCmd, 100),
		chanGetDict: make(chan *GetDictCmd, 100),
		chanBackup:  make(chan BackupCmd, 100),
	}

	if err := this.load(); err != nil {
		return err
	}
	exited.Add(1)
	go func() {
		defer exited.Done()
		this.wg.Add(1)
		go this.syncLoop(&this.wg)
		for i := 0; i < 3; i++ {
			this.wg.Add(1)
			go this.flowLoop(&this.wg)
		}
		this.wg.Add(1)
		go this.backupLoop(&this.wg)

		mux := http.NewServeMux()
		pattern := "/" + this.ID + "/write/"
		mux.Handle(pattern, http.StripPrefix(pattern, nodeWrite{dataNode: this}))
		this.AddMux("/"+this.Preffix+"/write/", pattern, false)
		pattern = "/" + this.ID + "/query"
		mux.Handle(pattern, http.StripPrefix(pattern, nodeQuery{dataNode: this}))
		this.AddMux("/"+this.Preffix+"/query", pattern, true)
		println("data server at:http://" + args.Host + ":" + strconv.FormatInt(int64(args.Port), 10) + "/" + this.ID)
		web := http.Server{Addr: ":" + strconv.FormatInt(int64(this.Port), 10), Handler: mux}
		this.wg.Add(1)
		go func() {
			defer this.wg.Done()
			if err := web.ListenAndServe(); err != http.ErrServerClosed {
				errs.Println(err)
			}
		}()
		this.Type, this.Status = NtData, NsStart
		for {
			if err = Hookup(this.Coord, &this.Node, true); err != nil {
				errs.Println(err)
				time.Sleep(time.Second * 3)
			} else {
				break
			}
		}
		ti := time.NewTicker(time.Second * 3)
		for {
			select {
			case <-this.ctx.Done():
				if err := web.Shutdown(context.Background()); err != nil {
					errs.Println(err)
				}
				close(this.chanData)
				close(this.chanBackup)
				close(this.chanGetFlow)
				close(this.chanGetDict)
				this.wg.Wait()
				return
			case <-ti.C:
				if err = Hookup(this.Coord, &this.Node, false); err != nil {
					errs.Println(err)
				}
			}
		}
	}()
	return nil
}

func (this *dataNode) GetFlow(name string) (*Flow, error) {
	if flow, ok := this.flows.Load(name); ok {
		return flow.(*Flow), nil
	}
	cmd := GetFlowerCmd{name: name}
	cmd.wg.Add(1)
	this.chanGetFlow <- &cmd
	cmd.wg.Wait()
	return cmd.flow, cmd.err
}
func (this *dataNode) GetDict(name string) (*mut.DataFrame, error) {

	if dict, ok := this.dicts.Load(name); ok {
		return dict.(*Dict).DataFrame, nil
	}
	cmd := GetDictCmd{name: name}
	cmd.wg.Add(1)
	this.chanGetDict <- &cmd
	cmd.wg.Wait()
	return cmd.dict, cmd.err
}
func (this *dataNode) NewFrame(name string) (*mut.DataFrame, error) {
	if flow, err := this.GetFlow(name); err != nil {
		return nil, err
	} else if flow != nil && flow.Desc != nil {
		return mut.NewFrame(flow.Desc.Time, flow.Desc.Dimensions, flow.Desc.Metrics), nil
	} else if uint, err := this.GetUint(name, true); err != nil {
		return nil, err
	} else if desc := uint.GetDesc(); desc != nil {
		return mut.NewFrame(desc.Time, desc.Dimensions, desc.Metrics), nil
	} else {
		return mut.AnyFrame(), nil
	}
}

func (this *dataNode) GetUint(name string, create bool) (uint *DataUnit, err error) {
	var etag string
	var desc *mut.Desc
	if r, ok := this.uints.Load(name); ok {
		return r.(*DataUnit), err
	} else if !create {
		return
	} else if _, etag, err = Download(this.Coord+DescsDir+name+DescExt, &desc, "", ""); err != nil {
		return
	} else if uint, err = NewDataUnit(this, this.ctx, &this.wg, name, desc, etag, nil); err != nil {
		return
	} else if r, ok = this.uints.LoadOrStore(name, uint); ok {
		uint.Close()
		return r.(*DataUnit), nil
	} else {
		return uint, nil
	}
}
func DecodeFileName(v string) (uint64, int64, int64, error) {
	if i := strings.Index(v, "."); i >= 0 {
		v = v[0:i]
	}
	if bs, err := base64.URLEncoding.DecodeString(v); err != nil {
		return 0, 0, 0, errs.New(err)
	} else if len(bs) != 24 {
		return 0, 0, 0, errs.New("length is error!")
	} else {
		return binary.BigEndian.Uint64(bs[0:8]),
			int64(binary.BigEndian.Uint64(bs[8:16])),
			int64(binary.BigEndian.Uint64(bs[16:24])), nil
	}
}
func EncodeFileName(id uint64, start, end int64) string {
	var bs [24]byte
	binary.BigEndian.PutUint64(bs[0:8], id)
	binary.BigEndian.PutUint64(bs[8:16], uint64(start))
	binary.BigEndian.PutUint64(bs[16:24], uint64(end))
	return base64.URLEncoding.EncodeToString(bs[:])
}
func (this *dataNode) loadFiles(dir string, files map[uint64]HistoryFrame) (map[uint64]HistoryFrame, error) {
	if fs, err := ioutil.ReadDir(dir); err != nil {
		return files, errs.New(err)
	} else {
		for i := range fs {
			if !fs[i].IsDir() {
				if id, s, e, err := DecodeFileName(fs[i].Name()); err == nil {
					if files == nil {
						files = make(map[uint64]HistoryFrame)
					}
					files[id] = HistoryFrame{ID: id, Start: s, End: e, Remote: false}
				}
			}
		}
	}
	return files, nil
}
func (this *dataNode) loadLocal(dir string, files map[string]map[uint64]HistoryFrame) (map[string]map[uint64]HistoryFrame, error) {
	if fs, err := ioutil.ReadDir(dir); err != nil {
		if os.IsNotExist(err) {
			return files, nil
		} else {
			return files, err
		}
	} else {
		for i := range fs {
			if fs[i].IsDir() {
				if files[fs[i].Name()], err = this.loadFiles(path.Join(dir, fs[i].Name()), files[fs[i].Name()]); err != nil {
					return files, err
				}
			}
		}
	}
	return files, nil
}
func (this *dataNode) loadRemote(addr string, id string, files map[string]map[uint64]HistoryFrame) (map[string]map[uint64]HistoryFrame, error) {
	var ok bool
	var dst map[uint64]HistoryFrame
	var fs map[string][]FileInfo
	resp, err := http.Get(addr + "/list/" + id)
	if err != nil {
		return files, errs.New(err)
	}
	defer resp.Body.Close()
	if bs, err := ioutil.ReadAll(resp.Body); err != nil {
		return files, errs.New(err)
	} else if resp.StatusCode == http.StatusNotFound {
		return files, nil
	} else if resp.StatusCode != http.StatusOK {
		return files, errs.New(string(bs))
	} else if err := json.Unmarshal(bs, &fs); err != nil {
		return files, errs.New(err)
	}
	for name, src := range fs {
		if dst, ok = files[name]; !ok {
			dst = make(map[uint64]HistoryFrame)
		}
		for i := range src {
			if id, s, e, err := DecodeFileName(src[i].Name); err != nil {
				return files, err
			} else {
				dst[id] = HistoryFrame{ID: id, Start: s, End: e, Remote: true}
			}
		}
		files[name] = dst
	}
	return files, nil
}

func (this *dataNode) load() (err error) {
	files := make(map[string]map[uint64]HistoryFrame)
	if this.StoreAddr != "" {
		if files, err = this.loadRemote(this.StoreAddr, this.ID, files); err != nil {
			return
		}
	}
	if files, err = this.loadLocal(path.Join(this.Dir, this.ID), files); err != nil {
		return
	}

	for name, fs := range files {
		var desc *mut.Desc
		if _, etag, err := Download(this.Coord+DescsDir+name+DescExt, &desc, "0", ""); err != nil {
			return err
		} else if unit, err := NewDataUnit(this, this.ctx, &this.wg, name, desc, etag, fs); err != nil {
			return err
		} else {
			this.uints.Store(name, unit)
		}
	}
	return nil
}
func (this *dataNode) syncFlows(flows []string) ([]string, error) {
	var name string
	var flow *Flow
	if len(flows) == 0 {
		this.flows.Range(func(key, value interface{}) bool {
			flows = append(flows, key.(string))
			return true
		})
	}
	if len(flows) > 0 {
		name, flows = flows[0], flows[1:]
		if old, ok := this.flows.Load(name); ok {
			if code, etag, err := Download(this.Coord+FlowsDir+name+FlowExt, &flow, old.(*Flow).etag, ""); err != nil {
				return flows, err
			} else if code == http.StatusNotFound {
				this.flows.Delete(name)
				return flows, nil
			} else if code == http.StatusOK {
				if f, err := flow.init(etag); err != nil {
					return flows, err
				} else {
					f.etag = etag
					this.flows.Store(name, f)
					return flows, nil
				}
			}
		}
	}
	return flows, nil
}

func (this *dataNode) syncDicts(dicts []string) ([]string, error) {
	var name string
	if len(dicts) == 0 {
		this.dicts.Range(func(key, value interface{}) bool {
			dicts = append(dicts, key.(string))
			return true
		})
	}
	if len(dicts) > 0 {
		name, dicts = dicts[0], dicts[1:]
		if old, ok := this.dicts.Load(name); ok {
			if frame, etag, err := mut.LoadFrame(this.Coord+DictsDir+name+DictExt, old.(*Dict).etag); err != nil {
				return dicts, err
			} else if frame == nil {
				return dicts, nil
			} else if err = frame.Reindex(); err == nil {
				this.dicts.Store(name, &Dict{DataFrame: frame, etag: etag})
				old.(*Dict).DataFrame.Close()
			}
		}
	}
	return dicts, nil
}
func (this *dataNode) syncLoop(exited *sync.WaitGroup) {
	defer exited.Done()
	var err error
	var flow *Flow
	flows := make([]string, 0)
	dicts := make([]string, 0)
	timer := time.NewTimer(time.Second * 5)
	for {
		select {
		case cmd, ok := <-this.chanGetFlow:
			if !ok {
				return
			}
			cmd.flow, cmd.err = nil, nil
			if old, ok := this.flows.Load(cmd.name); ok {
				if old != nil {
					cmd.flow = old.(*Flow)
				}
			} else if code, etag, err := Download(this.Coord+FlowsDir+cmd.name+FlowExt, &flow, "", ""); err != nil {
				cmd.err = err
			} else if code == http.StatusOK {
				if cmd.flow, cmd.err = flow.init(etag); cmd.err == nil {
					flow.etag = etag
					this.flows.Store(cmd.name, flow)
				}
			}
			cmd.wg.Done()
		case cmd, ok := <-this.chanGetDict:
			if !ok {
				return
			}
			cmd.dict, cmd.err = nil, nil
			if old, ok := this.dicts.Load(cmd.name); ok {
				if old != nil {
					cmd.dict = old.(*Dict).DataFrame
				}
			} else if frame, etag, err := mut.LoadFrame(this.Coord+DictsDir+cmd.name+DictExt, ""); err != nil {
				cmd.err = err
			} else if frame == nil {
				cmd.err = errs.New(cmd.name + " dict not found!")
			} else if err = frame.Reindex(); err == nil {
				cmd.dict = frame
				this.dicts.Store(cmd.name, &Dict{DataFrame: frame, etag: etag})
			} else {
				cmd.err = err
			}
			cmd.wg.Done()
		case <-timer.C:
			if flows, err = this.syncFlows(flows); err != nil {
				errs.Println(err)
			}
			if flows, err = this.syncDicts(dicts); err != nil {
				errs.Println(err)
			}
			timer.Reset(time.Second * 3)
		}
	}
}
func (this *dataNode) flowLoop(exited *sync.WaitGroup) {
	defer exited.Done()
	for {
		select {
		case cmd, ok := <-this.chanData:
			if !ok {
				return
			}
			if flow, err := this.GetFlow(cmd.name); err != nil {
				cmd.data.Close()
				if cmd.completed(err) {
					errs.Println(err)
				}
			} else if flow != nil {
				cmd.doFlow(this, flow)
			} else if unit, err := this.GetUint(cmd.name, true); err != nil {
				cmd.data.Close()
				if cmd.completed(err) {
					errs.Println(err)
				}
			} else {
				unit.chanData <- cmd
			}
		}
	}
}
func (this *dataNode) syncWrited(name string, writed int64) {
	/*
		if _, err := utils.Hookup(this.Coord+"/watch/data/writed/"+name+"/"+this.ID, writed, ""); err != nil {
			errs.Println(err)
		}
	*/
}

func (this *dataNode) SaveLocal(frame *mut.DataFrame, name string) error {
	s, e := frame.Periods()
	name = "file://" + path.Join(this.Dir, this.ID, name, EncodeFileName(frame.ID(), s, e)+".tar.gz")
	return frame.Save(name)
}

func (this *dataNode) LoadFrame(name string, id uint64, start int64, end int64, remote bool) (*mut.DataFrame, error) {
	var addr string
	if remote && this.StoreAddr != "" {
		addr = this.StoreAddr + "/store/" + this.ID + "/" + name + "/" + EncodeFileName(id, start, end) + ".tar.gz"
	} else {
		addr = "file://" + path.Join(this.Dir, this.ID, name, EncodeFileName(id, start, end)+".tar.gz")
	}
	frame, _, err := mut.LoadFrame(addr, "")
	return frame, err
}
func (this *dataNode) backupLoop(exited *sync.WaitGroup) {
	defer exited.Done()
	for {
		select {
		case cmd, ok := <-this.chanBackup:
			if !ok {
				return
			}
			fileName := EncodeFileName(cmd.ID, cmd.Start, cmd.End) + ".tar.gz"
			localFile := this.Dir + "/" + path.Join(this.ID, cmd.Name, fileName)
			remoteAddr := this.StoreAddr + "/" + this.ID + "/" + cmd.Name + "/" + fileName
			if f, err := os.Open(localFile); err == nil {
				if resp, err := http.Post(remoteAddr, "application/x-www-form-urlencoded", f); err != nil {
					errs.Println(err)
				} else {
					if bs, err := ioutil.ReadAll(resp.Body); err != nil {
						errs.Println(err)
					} else if resp.StatusCode != 200 {
						errs.Println(string(bs))
					}
					if err = resp.Body.Close(); err != nil {
						errs.Println(err)
					}
				}
				_ = f.Close()
			}
		}
	}
}
