package betel

import (
	"bytes"
	"io/ioutil"
	"strconv"
	"testing"
)

func TestLoadBetel(t *testing.T) {
	if bs, err := ioutil.ReadFile("C:\\Users\\why76\\OneDrive\\src\\ares\\betel.py"); err != nil {
		t.Log(err)
	} else {
		buff := bytes.NewBuffer(nil)
		buff.WriteString("package res\r\n")
		buff.WriteString("var PyBetel = []byte{")
		for i := range bs {
			if i != 0 {
				buff.WriteByte(',')
			}
			if i%10 == 0 {
				buff.WriteString("\r\n")
			}
			buff.WriteString("0x")
			buff.WriteString(strconv.FormatInt(int64(bs[i]), 16))
		}
		buff.WriteByte('}')
		println(buff.String())
	}
}
