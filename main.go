package main

import (
	"betel/errs"
	"betel/vars"
)

func main(){
	println(vars.ID())
	println(errs.New("111"))
}
