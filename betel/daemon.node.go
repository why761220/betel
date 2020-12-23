package betel

import (
	"betel/errs"
	"bufio"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Process struct {
	id     string
	cmd    *exec.Cmd
	Name   string
	Args   []string
	exited int64
	log    *os.File
}

func (this *Process) Kill() (err error) {
	if this.cmd != nil && this.cmd.Process != nil {
		if this.cmd.ProcessState == nil || !this.cmd.ProcessState.Exited() {
			if err = this.cmd.Process.Kill(); err != nil {
				return err
			} else {
				this.cmd = nil
				if this.log != nil {
					this.log.Close()
					this.log = nil
				}
			}
		}
	}
	return
}
func (this *Process) Close() {
	if this.cmd != nil && this.cmd.Process != nil {
		_ = this.cmd.Process.Kill()
		this.cmd = nil
	}
	if this.log != nil {
		this.log.Close()
		this.log = nil
	}
}
func (this *Process) PID() int {
	if this.cmd != nil && this.cmd.Process != nil {
		return this.cmd.Process.Pid
	} else {
		return -1
	}
}
func (this *Process) State() string {
	if this.cmd == nil || this.cmd.Process == nil {
		return "no init"
	} else if this.cmd.ProcessState == nil {
		return "running"
	} else if this.cmd.ProcessState.Exited() {
		return "exit:" + strconv.FormatInt(int64(this.cmd.ProcessState.ExitCode()), 10)
	} else {
		return "unkown"
	}
}

func StartProcess(name string, args ...string) (proc *Process, err error) {
	var log *os.File
	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	_ = os.MkdirAll("./logs", os.ModePerm)
	if log, err = os.Create("./logs/" + id + ".log"); err != nil {
		return
	}
	cmd := exec.Command(name, args...)
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		return
	}
	proc = &Process{id: id, Name: name, cmd: cmd, Args: args, log: log}
	go func() {
		if err := cmd.Wait(); err != nil {
			errs.Println(err)
		}
		atomic.StoreInt64(&proc.exited, 1)
	}()
	return proc, nil
}

type Daemon struct {
	*Node
	procs sync.Map
}

func (this *Daemon) DownloadBetelPy() (fileName string, err error) {
	var req *http.Request
	var resp *http.Response
	var file *os.File
	var fs os.FileInfo
	var bs []byte
	download := func(ti time.Time) (err error) {
		if req, err = http.NewRequest("GET", this.Coord+"/conf/py/betel.py", nil); err != nil {
			return
		}
		req.Header.Set("If-Modified-Since", ti.Format(http.TimeFormat))
		if resp, err = http.DefaultClient.Do(req); err != nil {
			return
		}
		defer resp.Body.Close()
		if bs, err = ioutil.ReadAll(resp.Body); err != nil {
			return
		}
		switch resp.StatusCode {
		case http.StatusOK:
			err = ioutil.WriteFile(fileName, bs, os.ModePerm)
			return
		case http.StatusNotModified:
			return
		default:
			err = errs.New(string(bs))
			return
		}
	}
	if fileName, err = filepath.Abs(filepath.Join(this.Dir, "py", "betel.py")); err != nil {
		return
	} else if file, err = os.Open(fileName); err == nil {
		defer file.Close()
		if fs, err = file.Stat(); err == nil {
			return fileName, download(fs.ModTime())
		} else {
			return fileName, download(time.Time{})
		}
	} else {
		return fileName, download(time.Time{})
	}
}

func (this *Daemon) startProcess(name string, w http.ResponseWriter, r *http.Request, args ...string) {
	var bs []byte
	var err error
	var kwArgs Args
	var proc *Process
	if bs, err = ioutil.ReadAll(r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if err = json.Unmarshal(bs, &kwArgs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		args = append(append([]string{}, args...), kwArgs.cmdArgs()...)
		if proc, err = StartProcess(name, args...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			this.procs.Store(proc.id, proc)
			HttpResult(w, Args{"id": proc.id})
		}
	}
}
func (this *Daemon) startPyBetel(w http.ResponseWriter, r *http.Request) {
	if fileName, err := this.DownloadBetelPy(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		this.startProcess("python", w, r, "-u", fileName)
	}
}
func (this *Daemon) pipList(w http.ResponseWriter, r *http.Request) {
	cmd := exec.Command("pip", "list")
	if bs, err := cmd.CombinedOutput(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		_, _ = w.Write(bs)
	}
}
func (this *Daemon) pipInstall(w http.ResponseWriter, r *http.Request) {
	if err := SyncDir(this.Coord+"/conf/whl", "./conf/whl"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	args := append([]string{}, "install")
	args = append(args, strings.Split(r.URL.Path, "/")...)
	args = append(args, "-f", "./conf/whl", "--no-index")
	cmd := exec.Command("pip", args...)
	if bs, err := cmd.CombinedOutput(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		_, _ = w.Write(bs)
	}
}
func (this *Daemon) startPython(w http.ResponseWriter, r *http.Request) {
	this.startProcess("python", w, r, "-u")
}
func (this *Daemon) startSelf(w http.ResponseWriter, r *http.Request) {
	this.startProcess(os.Args[0], w, r, r.URL.Path)
}
func (this *Daemon) stop(w http.ResponseWriter, r *http.Request) {
	if id := r.URL.Path; id == "" {
		http.Error(w, "id is empty!", http.StatusInternalServerError)
	} else if cmd, ok := this.procs.Load(id); ok {
		if err := cmd.(*Process).Kill(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			this.procs.Delete(id)
		}
	} else {
		http.Error(w, "id:"+id+" not found!", http.StatusInternalServerError)
	}
}

func (this *Daemon) procList(w http.ResponseWriter, r *http.Request) {
	var result []Args
	this.procs.Range(func(key, value interface{}) bool {
		proc := value.(*Process)
		result = append(result, Args{"id": key, "name": proc.Name, "pid": proc.PID(), "args": proc.Args, "state": proc.State()})
		return true
	})
	HttpResult(w, result)
}

func (this *Daemon) procLog(w http.ResponseWriter, r *http.Request) {
	if id := r.URL.Path; id == "" {
		http.Error(w, "id is empty!", http.StatusInternalServerError)
	} else if t, ok := this.procs.Load(id); ok {
		proc := t.(*Process)
		if proc.log == nil {
			http.Error(w, "not find log file!", http.StatusInternalServerError)
			return
		} else {
			w.Header().Set("content-type", "text/plain")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("Transfer-Encoding", "chunked")
			if file, err := os.Open(proc.log.Name()); err != nil {
				http.Error(w, "not find log file!", http.StatusInternalServerError)
			} else {
				defer file.Close()
				read := bufio.NewReader(file)
				var line []byte
				for {
					if line, _, err = read.ReadLine(); err != nil && err != io.EOF {
						http.Error(w, "not find log file!", http.StatusInternalServerError)
						return
					} else if err == io.EOF {
						if atomic.LoadInt64(&proc.exited) != 0 {
							return
						} else {
							time.Sleep(time.Millisecond * 100)
						}
					} else if _, err = w.Write(line); err != nil {
						return
					} else if _, err = w.Write([]byte{'\r', '\n'}); err != nil {
						return
					} else if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}

				}
			}
		}
	} else {
		http.Error(w, "id:"+id+" not found!", http.StatusInternalServerError)
	}
}

func StartDaemon(ctx context.Context, exited *sync.WaitGroup, args StartArgs) error {
	var wg sync.WaitGroup
	this := Daemon{
		Node: (*Node)(&args),
	}
	mux := http.NewServeMux()
	pattern := "/" + this.ID + "/proc/list"
	mux.HandleFunc(pattern, this.procList)
	pattern = "/" + this.ID + "/proc/log/"
	mux.Handle(pattern, http.StripPrefix(pattern, http.HandlerFunc(this.procLog)))
	pattern = "/" + this.ID + "/proc/start/py/betel"
	mux.HandleFunc(pattern, this.startPyBetel)
	pattern = "/" + this.ID + "/proc/start/python"
	mux.HandleFunc(pattern, this.startPython)
	pattern = "/" + this.ID + "/proc/start/betel/"
	mux.Handle(pattern, http.StripPrefix(pattern, http.HandlerFunc(this.startSelf)))
	pattern = "/" + this.ID + "/proc/stop/"
	mux.Handle(pattern, http.StripPrefix(pattern, http.HandlerFunc(this.stop)))
	pattern = "/" + this.ID + "/pip/list"
	mux.HandleFunc(pattern, this.pipList)
	pattern = "/" + this.ID + "/pip/install/"
	mux.Handle(pattern, http.StripPrefix(pattern, http.HandlerFunc(this.pipInstall)))
	println("daemon server at:http://" + args.Host + ":" + strconv.FormatInt(int64(args.Port), 10) + "/" + this.ID)
	srv := &http.Server{Addr: ":" + strconv.FormatInt(int64(args.Port), 10), Handler: mux}
	go func() {
		wg.Add(1)
		defer wg.Done()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			errs.Println(err)
		}
	}()
	exited.Add(1)
	go func() {
		defer exited.Done()
		for {
			select {
			case <-ctx.Done():
				if srv != nil {
					if err := srv.Shutdown(context.Background()); err != nil {
						errs.Println(err)
					}
				}
				this.procs.Range(func(key, value interface{}) bool {
					this.procs.Delete(key)
					value.(*Process).Close()
					return true
				})
				return
			}
		}
	}()
	return nil
}
