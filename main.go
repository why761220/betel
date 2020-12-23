package main

import (
	"betel/errs"
	vars "betel/vars"
)

func main() {
	println(vars.NewID())
	println(errs.New("111"))
}
