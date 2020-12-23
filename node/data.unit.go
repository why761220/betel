package node

import (
	"betel/errs"
	mut "betel/vars"
	"context"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type DataUnit struct {
	name     string
	desc     *mut.Desc
	etag     string
	writed   int64
	chanData chan *RecvCmd
	worker   []UnitWorker
}
type BackupCmd struct {
	Name  string
	ID    uint64
	Start int64
	End   int64
}

type HistoryFrame struct {
	*mut.DataFrame
	ID       uint64
	Start    int64
	End      int64
	Remote   bool
	LastTime time.Time
	ETag     string
}
type UnitWorker struct {
	chanQuery chan *QueryCmd
	chanDesc  chan *mut.Desc
}

func NewDataUnit(node *dataNode, ctx context.Context, exited *sync.WaitGroup, name string, desc *mut.Desc, etag string, unloads map[uint64]HistoryFrame) (*DataUnit, error) {

	this := &DataUnit{
		name:     name,
		desc:     desc,
		etag:     etag,
		chanData: make(chan *RecvCmd, 1000),
		worker:   make([]UnitWorker, 0, 5),
	}

	his := this.split(3, unloads)
	for i := 0; i < 3; i++ {
		worker := UnitWorker{
			chanQuery: make(chan *QueryCmd, 10),
			chanDesc:  make(chan *mut.Desc, 10),
		}
		exited.Add(1)
		this.worker = append(this.worker, worker)
		go worker.workLoop(node, this, desc, exited, his[i])
	}
	exited.Add(1)
	go func() {
		defer exited.Done()
		var writed int64
		ti := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-ctx.Done():
				close(this.chanData)
				return
			case <-ti.C:
				var desc mut.Desc
				if code, etag, err := Download(node.Coord+DescsDir+this.name+DescExt, &desc, this.etag, ""); err != nil {
					errs.Println(err)
				} else if code == http.StatusOK {
					for i := range this.worker {
						this.worker[i].chanDesc <- &desc
					}
					this.etag = etag
				} else if code == http.StatusNotFound {
					for i := range this.worker {
						this.worker[i].chanDesc <- nil
					}
				}
				if w := atomic.LoadInt64(&this.writed); w != writed {
					writed = w
					node.syncWrited(this.name, w)
				}
			}
		}
	}()
	return this, nil
}

func (this DataUnit) split(count int, unloads map[uint64]HistoryFrame) []map[uint64]HistoryFrame {
	ret := make([]map[uint64]HistoryFrame, count)
	for i := range ret {
		ret[i] = make(map[uint64]HistoryFrame)
	}
	i := 0
	for id, fi := range unloads {
		ret[i%count][id] = fi
		i++
	}
	return ret
}

func (this DataUnit) Query(cmd *QueryCmd) {
	for i := range this.worker {
		cmd.wg.Add(1)
		this.worker[i].chanQuery <- cmd
	}
}

func (this DataUnit) GetDesc() *mut.Desc {
	if t := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.desc))); t != nil {
		return (*mut.Desc)(t)
	} else {
		return nil
	}
}

func (this *DataUnit) addWrited(v int) {
	atomic.AddInt64(&this.writed, int64(v))
}

func (this DataUnit) Close() {
	close(this.chanData)
}

func InDuration(s1, e1, s2, e2 int64) bool {
	if e1 <= 0 {
		e1 = math.MaxInt64
	}
	if e2 <= 0 {
		e2 = math.MaxInt64
	}
	if s1 < s2 {
		return s2 <= e1
	} else if s1 > s2 {
		return e2 >= s1
	} else {
		return true
	}
}

func (this UnitWorker) workLoop(node *dataNode, unit *DataUnit, desc *mut.Desc, exit *sync.WaitGroup, histories map[uint64]HistoryFrame) {
	defer exit.Done()
	var err error
	var writed int
	var current *mut.DataFrame
	if desc != nil {
		current = mut.NewFrame(desc.Time, desc.Dimensions, desc.Metrics)
	} else {
		current = mut.AnyFrame()
	}
	if histories == nil {
		histories = make(map[uint64]HistoryFrame)
	}
	ti := time.NewTicker(time.Minute)
	for {
		select {
		case cmd, ok := <-unit.chanData:
			if !ok {
				if current.Count() > 0 {
					if err := node.SaveLocal(current, unit.name); err != nil {
						errs.Println(err)
					}
				}
				return
			}
			if err = mut.Merge(current, cmd.data, nil, nil); err != nil {
				cmd.err = err
			}
			cmd.next(node)
			//
			if w := current.Writed(); w != writed {
				unit.addWrited(w - writed)
				writed = w
			}
			if current.Writed() >= 10*100*10000 || current.Count() >= 100*10000 {
				if histories, err = this.Save(node, unit, current, histories); err != nil {
					errs.Println(err)
				}
				if desc != nil {
					current = mut.NewFrame(desc.Time, desc.Dimensions, desc.Metrics)
				} else {
					current = current.Clone(false)
				}
				writed = 0
			}
		case cmd := <-this.chanQuery:
			if !cmd.Stopped() {
				if s1, e1 := current.Periods(); InDuration(s1, e1, cmd.Start, cmd.End) {
					if rows, err := current.Select(cmd.SelectArgs(desc)); err != nil {
						cmd.SetErr(err)
					} else if err = cmd.Write(current, rows); err != nil {
						cmd.SetErr(err)
					}
				}
			}
			histories = this.QueryHistories(node, desc, unit.name, cmd, histories)
			cmd.wg.Done()
		case newDesc := <-this.chanDesc:
			if (desc == nil && newDesc != nil) || (desc != nil && newDesc == nil) {
				if current.Count() > 0 {
					if histories, err = this.Save(node, unit, current, histories); err != nil {
						errs.Println(err)
					}
				}
				writed, desc = 0, newDesc
				if desc != nil {
					current = mut.NewFrame(desc.Time, desc.Dimensions, desc.Metrics)
				} else {
					current = current.Clone(false)
				}
			}
		case t := <-ti.C:
			for id, frame := range histories {
				if frame.DataFrame != nil && t.Sub(frame.LastTime) > time.Minute*30 {
					frame.DataFrame.Close()
					frame.DataFrame = nil
					histories[id] = frame
				}
			}
		}
	}
}
func (this UnitWorker) Save(node *dataNode, unit *DataUnit, current *mut.DataFrame, histories map[uint64]HistoryFrame) (map[uint64]HistoryFrame, error) {
	if err := node.SaveLocal(current, unit.name); err != nil {
		return histories, err
	} else {
		start, end := current.Periods()
		his := HistoryFrame{DataFrame: current, Start: start, End: end, LastTime: time.Now()}
		node.chanBackup <- BackupCmd{Name: unit.name, ID: current.ID(), Start: start, End: end}
		histories[current.ID()] = his
		return histories, nil
	}
}
func (this UnitWorker) QueryHistories(node *dataNode, desc *mut.Desc, name string, cmd *QueryCmd, histories map[uint64]HistoryFrame) map[uint64]HistoryFrame {
	var err error
	for id, info := range histories {
		if cmd.Stopped() {
			return histories
		}
		if InDuration(info.Start, info.End, cmd.Start, cmd.End) {
			if info.DataFrame == nil {
				if info.DataFrame, err = node.LoadFrame(name, info.ID, info.Start, info.End, info.Remote); err != nil {
					cmd.SetErr(err)
					return histories
				}
			}

			if rows, err := info.Select(cmd.SelectArgs(desc)); err != nil {
				cmd.SetErr(err)
				return histories
			} else if rows != nil && rows.Len() > 0 {
				if err = cmd.Write(info.DataFrame, rows); err != nil {
					cmd.SetErr(err)
					return histories
				} else {
					info.LastTime = time.Now()
					histories[id] = info
				}
			}
		}
	}
	return histories
}
