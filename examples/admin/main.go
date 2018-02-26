package main

import (
	"log"

	"github.com/zhanghjster/go-gearman"
)

func main() {
	var server = "127.0.0.1:4730"

	var admin = gearman.NewAdmin([]string{server})

	// show version
	lines, err := admin.Do(server, gearman.AdmOptVersion())
	if err != nil {
		log.Fatal(err)
	}
	log.Println("version:")
	gearman.PrintLines(lines)

	// show workers
	lines, err = admin.Do(server, gearman.AdmOptWorkers())
	if err != nil {
		log.Fatal(err)
	}
	log.Println("workers:")
	gearman.PrintLines(lines)

	// show status
	lines, err = admin.Do(server, gearman.AdmOptStatus())
	if err != nil {
		log.Fatal(err)
	}
	log.Println("status")
	gearman.PrintLines(lines)
}
