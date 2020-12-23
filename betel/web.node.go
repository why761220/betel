package betel

import (
	"archive/zip"
	"betel/errs"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Web struct {
	Node
}

func (this Web) uploadHtml(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body>
<br/>
<p>web</p>
<form action="/` + this.ID + `/upload" method="post" enctype="multipart/form-data">
    <input type='file' name="files" multiple="multiple" accept=".zip"/>
    <input type="submit" />
</form>
</body>
</html>`))
}

func (this Web) upload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(1024 * 1024 * 32); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for key := range r.MultipartForm.File {
		files := r.MultipartForm.File[key]
		for _, file := range files {
			var name string
			if ext := filepath.Ext(file.Filename); ext != ".zip" {
				http.Error(w, file.Filename+" is not zip file", http.StatusInternalServerError)
				return
			} else {
				name = strings.TrimSuffix(file.Filename, ext)
			}
			if r, err := file.Open(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			} else if rz, err := zip.NewReader(r, file.Size); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			} else {
				for _, f := range rz.File {
					fpath := filepath.Join(this.Dir, name, f.Name)
					if f.FileInfo().IsDir() {
						if err = os.MkdirAll(fpath, os.ModePerm); err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
					} else {
						if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
						inFile, err := f.Open()
						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
						outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
						if err != nil {
							inFile.Close()
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
						_, err = io.Copy(outFile, inFile)
						if err != nil {
							inFile.Close()
							outFile.Close()
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
						inFile.Close()
						outFile.Close()
					}
				}
			}
		}
	}
}

func StartWeb(ctx context.Context, exited *sync.WaitGroup, args StartArgs) error {
	this := Web{
		Node: Node(args),
	}
	var wg sync.WaitGroup

	mux := http.NewServeMux()
	pattern := "/" + args.ID + "/upload"
	mux.HandleFunc(pattern, this.upload)
	this.AddMux(pattern, pattern, false)
	pattern = "/" + args.ID + "/upload.html"
	mux.HandleFunc(pattern, this.uploadHtml)
	this.AddMux(pattern, pattern, false)
	if fis, err := ioutil.ReadDir(args.Dir); err != nil {
		return err
	} else {
		for i := range fis {
			name := fis[i].Name()
			pattern = "/" + args.ID + "/" + name + "/"
			mux.Handle(pattern, http.StripPrefix(pattern, http.FileServer(http.Dir(filepath.Join(args.Dir, name)))))
			this.AddMux("/"+this.Preffix+"/"+name+"/", pattern, false)
		}
	}
	//pattern = "/" + args.ID + "/rts-web/"
	//mux.Handle(pattern, http.StripPrefix(pattern, http.FileServer(http.Dir(args.Dir+"/hainan/rts-web"))))
	//this.AddMux("/"+this.Preffix+"/rts-web/", pattern, false)
	//pattern = "/" + args.ID + "/screen/"
	//mux.Handle(pattern, http.StripPrefix(pattern, http.FileServer(http.Dir(args.Dir+"/hainan/screen"))))
	//this.AddMux("/"+this.Preffix+"/screen/", pattern, false)
	println("web server at:http://" + args.Host + ":" + strconv.FormatInt(int64(args.Port), 10) + "/" + this.Preffix + "/")
	web := http.Server{Addr: ":" + strconv.FormatInt(int64(args.Port), 10), Handler: mux}
	wg.Add(1)
	go func() {
		defer wg.Done()
		println("web server at: http://" + args.Host + ":" + strconv.FormatInt(int64(args.Port), 10) + "/" + args.ID)
		if err := web.ListenAndServe(); err != http.ErrServerClosed {
			errs.Println(err)
		}
	}()
	exited.Add(1)
	go func() {
		defer exited.Done()
		var err error
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
			case <-ctx.Done():
				if err := web.Shutdown(context.Background()); err != nil {
					errs.Println(err)
				}
				wg.Wait()
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
