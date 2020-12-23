package values

import (
	"betel/errs"
	"encoding/binary"
	"encoding/hex"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

var id, _ = SetNodeID(0)
var MAC = GetMAC()
var IP = LocalAddress()

func init() {
	rand.Seed(int64(id))
}
func NodeID() uint64 {
	return atomic.LoadUint64(&id)
}
func SetNodeID(v uint16) (id uint64, bs [8]byte) {
	n := copy(bs[:], MAC[:])
	binary.BigEndian.PutUint16(bs[n:], v)
	id = binary.BigEndian.Uint64(bs[:])
	return
}
func ID() uint64 {
	id := atomic.LoadUint64(&id)
	r, t := rand.Uint64(), uint64(time.Now().UnixNano())
	switch r % 4 {
	case 0:
		t = r + t ^ id + rand.Uint64()
	case 1:
		t = r | t + id ^ rand.Uint64()
	case 2:
		t = r*t ^ id*rand.Uint64()
	case 3:
		t = r ^ t*id&rand.Uint64()
	}
	return t
}
func ID16() string {
	var src [8]byte
	var dst [16]byte
	binary.BigEndian.PutUint64(src[:], ID())
	hex.Encode(dst[:], src[:])
	return string(dst[:])
}
func ID32() string {
	var dst [32]byte
	id := uuid.New()
	hex.Encode(dst[:], id[:])
	return string(dst[:])
}

func GetMAC() (ret [6]byte) {
	interfaces, err := net.Interfaces()
	if err != nil {
		errs.Println(err.Error())
		return
	}
	for _, inter := range interfaces {
		copy(ret[:], inter.HardwareAddr)
		return
	}
	return
}
func LocalAddress() string {
	conn, err := net.Dial("udp", "88.88.88.88:80")
	if err != nil {
		log.Println(err.Error())
		return "127.0.0.1"
	}
	defer func() {
		if err := conn.Close(); err != nil {
			errs.Println(err)
		}
	}()
	return strings.Split(conn.LocalAddr().String(), ":")[0]
}
