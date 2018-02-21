package gearman

import (
	"io/ioutil"
	"log"
	"os"
)

func init() {
	if os.Getenv("GE_CLIENT_DEBUG") == "1" {
		log.SetOutput(os.Stdout)
	} else {
		log.SetOutput(ioutil.Discard)
	}
}
