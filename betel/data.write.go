package betel

import (
	"betel/errs"
	mut "betel/vars"
	"io"
	"net/http"
	"strings"
	"sync"
)

type nodeWrite struct {
	*dataNode
}

func (this nodeWrite) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	var err error
	var frame *mut.DataFrame
	if request.Method != "POST" {
		http.Error(writer, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	fmt := mut.ParsePacketFormat(request.URL.Query().Get("fmt"), mut.PfJsonRow)
	name := strings.ReplaceAll(request.URL.Path, "/", ".")
	paths := func() (ret []string) {
		if t := request.URL.Query().Get("paths"); t != "" {
			ret = strings.Split(t, "/")
			if len(ret) > 0 && ret[0] == "" {
				ret = ret[1:]
			}
		}
		return
	}()

	if frame, err = this.NewFrame(name); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err = io.Copy(&mut.FrameWriter{DataFrame: frame, Format: fmt, Paths: paths}, request.Body); err != nil {
		frame.Close()
		errs.Println(err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	} else if request.URL.Query().Get("mode") == "sync" {
		var wg sync.WaitGroup
		wg.Add(1)
		cmd := &RecvCmd{data: frame, name: name, wg: &wg}
		this.chanData <- cmd
		wg.Wait()
		if cmd.err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		} else {
			println("writed: "+cmd.name+" for count:", cmd.data.Count())
		}
	} else {
		this.chanData <- &RecvCmd{data: frame, name: name}
	}
}
