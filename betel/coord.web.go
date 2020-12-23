package betel

import (
	mut "betel/vars"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unsafe"
)

var verifyCodes = [36]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A",
	"B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R",
	"S", "T", "U", "V", "W", "X", "Y", "Z"}
var verifiedCodes = sync.Map{}

type VerifyCode struct {
	code    string
	client  string
	expires time.Time
}

func (this *Coord) webGetIdentify(w http.ResponseWriter, r *http.Request) {
	var ret string
	w.Header().Set("Access-Control-Allow-Origin", "*")
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 4; i++ {
		index := rand.Intn(36)
		ret += verifyCodes[index]
	}
	code := &VerifyCode{code: ret, client: mut.NewID(), expires: time.Now().Add(time.Minute * 5)}
	verifiedCodes.Store(code.client, code)
	http.SetCookie(w, &http.Cookie{Path: "/", Name: "verify-client", Value: code.client, Expires: code.expires})
	if bs, err := json.Marshal(map[string]interface{}{"result": "success", "code": ret}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if _, err = w.Write(bs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
func (this *Coord) webUserAdd(writer http.ResponseWriter, request *http.Request) {
	var bs []byte
	var err error
	var user, users map[string]string
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	valid := func(s string) bool {
		var upper, lower, digit bool
		if len(s) < 8 {
			return false
		}
		rs := []rune(s)
		for i := range rs {
			if unicode.IsDigit(rs[i]) {
				digit = true
			} else if unicode.IsLower(rs[i]) {
				lower = true
			} else if unicode.IsUpper(rs[i]) {
				upper = true
			}
		}
		return upper && lower && digit
	}
	add := func() (err error) {
		var bs []byte
		name, password := user["name"], user["password"]
		if strings.TrimSpace(name) == "" {
			return errors.New("user/password is error")
		}
		if bs, err = base64.StdEncoding.DecodeString(password); err != nil {
			return err
		} else {
			password = string(bs)
		}
		if ps, ok := users[name]; ok {
			if old := user["old"]; old != "" {
				return errors.New("user/password is error")
			} else if bs, err = base64.StdEncoding.DecodeString(old); err != nil {
				return err
			} else {
				old = string(bs)
				if ps == old && valid(password) {
					users[name] = password
					return nil
				}
			}
		} else if valid(password) {
			users[name] = password
			return nil
		}
		return errors.New("user/password is error")
	}
	if bs, err = ioutil.ReadAll(request.Body); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	} else if err = json.Unmarshal(bs, &user); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	} else if bs, err = ioutil.ReadFile(path.Join(this.ConfDir, "users")); err != nil {
		if !os.IsNotExist(err) {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		} else {
			users = make(map[string]string)
		}
	} else if bs, err = Decrypt(bs); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	} else if err = json.Unmarshal(bs, &users); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
	if err = add(); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	} else if bs, err = json.Marshal(users); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	} else if bs, err = Encrypt(bs); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	} else if err = ioutil.WriteFile(path.Join(this.ConfDir, "users"), bs, os.ModePerm); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}
func (this *Coord) webUserList(w http.ResponseWriter, r *http.Request) {
	var err error
	var bs []byte
	var users map[string]string
	var ret []map[string]string
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if bs, err = ioutil.ReadFile(path.Join(this.ConfDir, "users")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if bs, err = Decrypt(bs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if err = json.Unmarshal(bs, &users); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		for name := range users {
			if name != "" {
				ret = append(ret, map[string]string{"name": name})
			}
		}
		if bs, err = json.Marshal(ret); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			_, _ = w.Write(bs)
		}
	}
}
func (this *Coord) webTokenGet(w http.ResponseWriter, r *http.Request) {
	var user User
	var bs []byte
	var token string
	var t interface{}
	var err error
	var ok bool
	var client *http.Cookie
	var users map[string]string
	w.Header().Set("Access-Control-Allow-Origin", "*")
	doVerfiy := func() error {
		if bs, err := base64.StdEncoding.DecodeString(user.Password); err != nil {
			return err
		} else if ps, ok := users[user.Name]; ok && (user.Password == ps || ps == string(bs)) {
			user.Password = ""
			user.Expire = time.Now().Add(time.Hour * 24 * 31).UnixNano()
			return nil
		} else {
			return errors.New("user/password is error")
		}
	}
	if bs, err = ioutil.ReadAll(r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else if client, err = r.Cookie("verify-client"); err != nil {
		_, _ = w.Write([]byte(`{"result": "1"}`))
		return
	} else if t, ok = verifiedCodes.Load(client.Value); !ok {
		_, _ = w.Write([]byte(`{"result": "1"}`))
		return
	} else {
		verifiedCodes.Delete(client)
	}
	if err = json.Unmarshal(bs, &user); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if code, ok := t.(*VerifyCode); !ok || code.expires.Before(time.Now()) || code.code != user.Code {
		_, _ = w.Write([]byte(`{"result": "1"}`))
	} else if bs, err = ioutil.ReadFile(path.Join(this.ConfDir, "users")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if bs, err = Decrypt(bs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if err = json.Unmarshal(bs, &users); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if err = doVerfiy(); err != nil {
		_, _ = w.Write([]byte(`{"result": "2"}`))
		return
	} else if bs, err = json.Marshal(user); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if token, err = EncryptToken(bs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		http.SetCookie(w, &http.Cookie{Path: "/", Name: "token", Value: token, Expires: time.Unix(0, user.Expire)})
		if bs, err := json.Marshal(struct {
			Result string `json:"result,omitempty"`
			Token  string `json:"token,omitempty"`
		}{Result: "0", Token: token}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			_, _ = w.Write(bs)
		}
	}
}
func (this *Coord) webProxyGet(w http.ResponseWriter, r *http.Request) {
	nodes := *(*map[string]*Node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.nodes))))
	var ret []*Node
	for k := range nodes {
		if nodes[k].Type == NtProxy {
			ret = append(ret, nodes[k])
		}
	}
	if bs, err := json.Marshal(ret); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if _, err = w.Write(bs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
