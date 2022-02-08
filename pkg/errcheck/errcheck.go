package errcheck

import (
	"fmt"
	"os"
)

func CheckErr(err error, msg ...string) {
	if err != nil {
		fmt.Println(msg, ": ", err.Error())
	}
}

func FatalErr(err error, msg ...string) {
	if err != nil {
		fmt.Println(msg, ": ", err.Error())
		os.Exit(10)
	}
}
