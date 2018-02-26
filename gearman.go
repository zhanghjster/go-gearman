package gearman

import (
	"io/ioutil"
	"log"
	"os"
)

var Log log.Logger

func init() {
	Log.SetFlags(log.LstdFlags)
	if os.Getenv("GM_DEBUG") == "1" {
		Log.SetOutput(os.Stdout)
	} else {
		Log.SetOutput(ioutil.Discard)
	}
}
