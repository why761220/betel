package betel

type NodeType int
type NodeStatus int

const (
	NtNone NodeType = iota
	NtCoord
	NtProxy
	NtData
	NtStore
	NtWeb
)
const (
	NsIdle NodeStatus = iota
	NsStart
	NsStop
	NsError
)

type Node struct {
	ID        string            `json:"id,omitempty"`
	Host      string            `json:"host,omitempty"`
	Port      int               `json:"port,omitempty"`
	TLSPort   int               `json:"tslPort,omitempty"`
	Preffix   string            `json:"preffix,omitempty"`
	Dir       string            `json:"dataDir,omitempty"`
	ConfDir   string            `json:"confDir,omitempty"`
	StoreAddr string            `json:"storeAddr,omitempty"`
	Coord     string            `json:"coord,omitempty"`
	CertFile  string            `json:"certFile,omitempty"`
	KeyFile   string            `json:"keyFile,omitempty"`
	Mapping   bool              `json:"mapping,omitempty"`
	Mux       map[string]Router `json:"mux,omitempty"`
	expires   int64
	Type      NodeType
	Status    NodeStatus
}
type StartArgs Node

func (this *Node) AddMux(uri, mapping string, isQuery bool) *Node {
	if this.Mux == nil {
		this.Mux = make(map[string]Router)
	}
	this.Mux[uri] = Router{URI: uri, Mapping: mapping, IsQuery: isQuery}
	return this
}
