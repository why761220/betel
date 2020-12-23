package betel

import (
	mut "betel/vars"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

const DescExt = ".json"
const FlowExt = ".json"
const DictExt = ".tar.gz"
const PythonExt = ".py"
const DescsName = "descs"
const FlowsName = "flows"
const DictsName = "dicts"
const PythonName = "py"
const WebDir = "./web"
const DescsDir = "/conf/" + DescsName + "/"
const FlowsDir = "/conf/" + FlowsName + "/"
const DictsDir = "/conf/" + DictsName + "/"
const PythonDir = "/conf/" + PythonName + "/"

type User struct {
	Name     string `json:"name,omitempty"`
	Password string `json:"password,omitempty"`
	Expire   int64  `json:"expire,omitempty"`
	Code     string `json:"code,omitempty"`
}

type FuncHandler struct {
	f http.HandlerFunc
}

func (f FuncHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if f.f != nil {
		f.f(writer, request)
	} else {
		http.NotFound(writer, request)
	}
}

type Auth struct {
	h http.Handler
}

func (a Auth) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, ok := UserVerify(r); ok {
		if a.h != nil {
			a.h.ServeHTTP(w, r)
		} else {
			http.Error(w, "", http.StatusInternalServerError)
		}
	} else {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	}
}
func UserVerify(r *http.Request) (string, bool) {
	var user User
	if t := r.URL.Query().Get("token"); t != "" {
		if err := DecryptToken(t, &user); err != nil {
			return user.Name, false
		} else {
			return user.Name, time.Now().Before(time.Unix(0, user.Expire))
		}
	} else if t := r.Header.Get("Authorization"); t != "" {
		if err := DecryptToken(t, &user); err != nil {
			return user.Name, false
		} else {
			return user.Name, time.Now().Before(time.Unix(0, user.Expire))
		}
	} else if cookie, err := r.Cookie("token"); err == nil {
		if err := DecryptToken(cookie.Value, &user); err != nil {
			return user.Name, false
		} else {
			return user.Name, time.Now().Before(time.Unix(0, user.Expire))
		}
	} else {
		return user.Name, false
	}
}

type FileServer struct {
	token bool
	root  string
	Get   func(root string, w http.ResponseWriter, r *http.Request)
	Post  func(root string, w http.ResponseWriter, r *http.Request)
}

func (this FileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	switch r.Method {
	case "GET":
		if this.Get != nil {
			this.Get(this.root, w, r)
		} else {
			this.defaultGet(w, r)
		}
	case "POST":
		if this.Post != nil {
			this.Post(this.root, w, r)
		} else {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		}
	default:
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}

func (this FileServer) defaultGet(w http.ResponseWriter, r *http.Request) {
	var fi os.FileInfo
	name := path.Join(this.root, path.Join(strings.Split(r.URL.Path, "/")...))
	if f, err := os.Open(name); err == nil {
		if fi, err = f.Stat(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			f.Close()
			return
		}
		modTime := fi.ModTime().Truncate(time.Second)
		if ius := r.Header.Get("If-Modified-Since"); ius != "" {
			if ti, err := http.ParseTime(ius); err == nil {
				ti = ti.Truncate(time.Second)
				if modTime.Equal(ti) || modTime.Before(ti) {
					http.Error(w, http.StatusText(http.StatusNotModified), http.StatusNotModified)
					f.Close()
					return
				}
			}
		}
		w.Header().Set("Last-Modified", modTime.UTC().Format(http.TimeFormat))
		if fi.IsDir() {
			if fis, err := ioutil.ReadDir(name); err == nil {
				if r.UserAgent() == "" || r.UserAgent() == "Go-http-client/1.1" {
					fs := make([]FileInfo, 0, len(fis))
					for i := range fis {
						fs = append(fs, FileInfo{}.From(fis[i]))
					}
					if err = json.NewEncoder(w).Encode(fs); err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
					}
				} else {
					sort.Slice(fis, func(i, j int) bool { return fis[i].Name() < fis[j].Name() })
					w.Header().Set("Content-Type", "text/html; charset=utf-8")
					fmt.Fprintf(w, "<pre>\n")
					for _, d := range fis {
						name := d.Name()
						if d.IsDir() {
							name += "/"
						}
						url := url.URL{Path: name}
						fmt.Fprintf(w, "<a href=\"%s\">%s</a>\n", url.String(), htmlReplacer.Replace(name))
					}
					fmt.Fprintf(w, "</pre>\n")
				}
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		} else if _, err := io.Copy(w, f); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		f.Close()
	} else if os.IsNotExist(err) {
		http.NotFound(w, r)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func DictPost(root string, w http.ResponseWriter, r *http.Request) {
	var cols []mut.Column
	dst := path.Join(root, path.Join(strings.Split(r.URL.Path, "/")...))
	fmt := mut.ParsePacketFormat(r.URL.Query().Get("fmt"), mut.PfJsonRow)
	if indexes := strings.Split(r.URL.Query().Get("indexes"), ","); len(indexes) > 0 {
		cols = make([]mut.Column, len(indexes))
		for i := range indexes {
			cols[i] = mut.Column{Name: indexes[i], Index: true}
		}
	}
	frame := mut.NewFrame(nil, nil, cols)
	defer frame.Close()
	fw := mut.FrameWriter{DataFrame: frame, Format: fmt}
	if _, err := io.Copy(fw, r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

	} else if err := frame.Save(dst); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
func SaveMultipartForm(root string, w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseMultipartForm(1024 * 1024 * 32); err != nil {
		return err
	}
	for key := range r.MultipartForm.File {
		files := r.MultipartForm.File[key]
		for _, file := range files {
			if r, err := file.Open(); err != nil {
				return err
			} else if bs, err := ioutil.ReadAll(r); err != nil {
				return err
			} else if err = ioutil.WriteFile(root+"/"+file.Filename, bs, os.ModePerm); err != nil {
				return err
			}
		}
	}
	return nil
}
func DescPost(root string, w http.ResponseWriter, r *http.Request) {
	var desc mut.Desc
	if strings.Index(r.Header.Get("Content-Type"), "multipart/form-data") >= 0 {
		if err := SaveMultipartForm(root, w, r); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		name := path.Join(root, path.Join(strings.Split(r.URL.Path, "/")...))
		if bs, err := ioutil.ReadAll(r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else if err = json.Unmarshal(bs, &desc); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else if err = desc.Verify(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else if err = ioutil.WriteFile(name, bs, os.ModePerm); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func FlowPost(root string, w http.ResponseWriter, r *http.Request) {
	var flow Flow
	if strings.Index(r.Header.Get("Content-Type"), "multipart/form-data") >= 0 {
		if err := SaveMultipartForm(root, w, r); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		name := path.Join(root, "descs", path.Join(strings.Split(r.URL.Path, "/")...))
		if bs, err := ioutil.ReadAll(r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else if err = json.Unmarshal(bs, &flow); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else if err = flow.Verify(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else if err = ioutil.WriteFile(name, bs, os.ModePerm); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
