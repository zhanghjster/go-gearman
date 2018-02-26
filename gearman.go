package gearman

import (
	"io/ioutil"
	"log"
	"os"
)

func init() {
	if os.Getenv("GM_DEBUG") == "1" {
		log.SetOutput(os.Stdout)
	} else {
		log.SetOutput(ioutil.Discard)
	}
}
