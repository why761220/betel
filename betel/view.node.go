package betel

import (
	"betel/errs"
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type View struct {
	Node
}

func StartView(ctx context.Context, exited *sync.WaitGroup, args StartArgs) error {
	this := View{
		Node: Node(args),
	}
	this.Type = NtWeb
	var wg sync.WaitGroup
	mux := http.NewServeMux()
	pattern := "/" + this.ID + "/view"
	mux.Handle(pattern, http.StripPrefix(pattern, http.FileServer(http.Dir(this.Dir))))
	this.AddMux("/"+this.Preffix+"/view", pattern, false)
	println("web server at:http://" + args.Host + ":" + strconv.FormatInt(int64(args.Port), 10) + "/" + this.Preffix + "/view")
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
func (this *View) add(w http.ResponseWriter, r *http.Request) {

}
