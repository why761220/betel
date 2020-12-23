package betel

import (
	"betel/errs"
	"bufio"
	"context"
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

var htmlReplacer = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	// "&#34;" is shorter than "&quot;".
	`"`, "&#34;",
	// "&#39;" is shorter than "&apos;" and apos was not in HTML until HTML5.
	"'", "&#39;",
)

type FileInfo struct {
	Name    string    `json:"name,omitempty"`
	Size    int64     `json:"size,omitempty"`
	Dir     bool      `json:"dir,omitempty"`
	ModTime time.Time `json:"modTime,omitempty"`
}

func (f FileInfo) From(fi os.FileInfo) FileInfo {
	f.Name = fi.Name()
	f.Size = fi.Size()
	f.Dir = fi.IsDir()
	f.ModTime = fi.ModTime()
	return f
}

type Store struct {
	*Node
}
type StoreList struct {
	*Node
}

func (this StoreList) listSub(name string) (fs []FileInfo, err error) {
	var dirs []os.FileInfo
	if dirs, err = ioutil.ReadDir(name); err == nil {
		for _, fi := range dirs {
			fs = append(fs, FileInfo{Name: fi.Name(), Dir: fi.IsDir(), ModTime: fi.ModTime(), Size: fi.Size()})
		}
	}
	return
}
func (this StoreList) list(name string) (ret map[string][]FileInfo, err error) {
	var (
		f  *os.File
		fi os.FileInfo
	)
	ret = make(map[string][]FileInfo)
	if f, err = os.Open(name); err != nil {
		if os.IsNotExist(err) {
			return ret, nil
		} else {
			return ret, err
		}
	}
	defer func() {
		if err := f.Close(); err != nil {
			errs.Println(err)
		}
	}()
	if fi, err = f.Stat(); err != nil {
		return ret, err
	} else if fi.IsDir() {
		if dirs, err := ioutil.ReadDir(name); err != nil {
			return ret, err
		} else {
			for _, fi := range dirs {
				if fi.IsDir() {
					if ret[fi.Name()], err = this.listSub(path.Join(name, fi.Name())); err != nil {
						return ret, err
					}
				}
			}
		}
	}
	return ret, nil
}
func (s StoreList) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch strings.ToUpper(r.Method) {
	case "GET":
		if ret, err := s.list(path.Join(s.Dir, r.URL.Path)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else if err := json.NewEncoder(w).Encode(ret); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	default:
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}

func (this Store) Get(w http.ResponseWriter, r *http.Request) (int, error) {
	var (
		f   *os.File
		fi  os.FileInfo
		err error
	)
	name := path.Join(this.Dir, r.URL.Path)
	if f, err = os.Open(name); err != nil {
		return http.StatusNotFound, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			errs.Println(err)
		}
	}()
	if fi, err = f.Stat(); err != nil {
		return http.StatusInternalServerError, err
	} else if fi.IsDir() {
		return http.StatusConflict, errs.New(fi.Name() + " is not file!")
	} else if _, err = bufio.NewReader(f).WriteTo(w); err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, err
}

func (b Store) Post(w http.ResponseWriter, r *http.Request) (int, error) {
	var (
		err error
		f   *os.File
	)
	name := path.Join(b.Dir, r.URL.Path)
	if err = os.MkdirAll(path.Dir(name), os.ModePerm); err != nil {
		return http.StatusInternalServerError, err
	} else if f, err = os.Create(name); err != nil {
		return http.StatusInternalServerError, err
	}
	_, err = bufio.NewReader(r.Body).WriteTo(f)
	if t := f.Close(); t != nil {
		errs.Println(t)
	}
	if err != nil {
		return http.StatusInternalServerError, err
	} else {
		return http.StatusOK, nil
	}
}

func (b Store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch strings.ToUpper(r.Method) {
	case "GET":
		if code, err := b.Get(w, r); err != nil {
			http.Error(w, err.Error(), code)
		}
	case "POST", "PUT":
		if code, err := b.Post(w, r); err != nil {
			http.Error(w, err.Error(), code)
		}
	default:
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}

func StartStore(ctx context.Context, exited *sync.WaitGroup, args StartArgs) (err error) {
	var wg sync.WaitGroup
	this := (*Node)(&args)
	mux := http.NewServeMux()
	pattern := "/" + this.ID + "/store/"
	mux.Handle(pattern, http.StripPrefix(pattern, &Store{Node: this}))
	pattern = "/" + this.ID + "/list/"
	mux.Handle(pattern, http.StripPrefix(pattern, &StoreList{Node: this}))
	this.AddMux(pattern, pattern, false)
	println("store server at:http://" + args.Host + ":" + strconv.FormatInt(int64(args.Port), 10) + "/" + this.ID)
	srv := &http.Server{Addr: ":" + strconv.FormatInt(int64(this.Port), 10), Handler: mux}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			errs.Println(err)
		}
	}()
	this.Type, this.Status = NtStore, NsStart
	exited.Add(1)
	go func() {
		defer exited.Done()
		var err error
		for {
			if err = Hookup(args.Coord, this, true); err == nil {
				break
			} else {
				time.Sleep(time.Second * 3)
			}
		}
		ti := time.NewTicker(time.Second * 30)
		for {
			select {
			case <-ctx.Done():
				if srv != nil {
					if err := srv.Shutdown(nil); err != nil {
						errs.Println(err)
					}
				}
				wg.Wait()
				return
			case <-ti.C:
				if err = Hookup(this.Coord, this, false); err != nil {
					errs.Println(err)
				}
			}
		}
	}()
	return
}
