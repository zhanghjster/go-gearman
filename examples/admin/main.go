package main

import (
	"log"

	"github.com/zhanghjster/go-gearman"
)

func main() {
	var server = []string{"127.0.0.1:4730", "localhost:4731"}

	var admin = gearman.NewAdmin(server)

	for _, s := range server {
		log.Printf("send admin commands to %s", s)

		// show version
		lines, err := admin.Do(s, gearman.AdmOptVersion())
		if err != nil {
			log.Fatal(err)
		}
		log.Println("version:")
		gearman.PrintLines(lines)

		// show workers
		lines, err = admin.Do(s, gearman.AdmOptWorkers())
		if err != nil {
			log.Fatal(err)
		}
		log.Println("workers:")
		gearman.PrintLines(lines)

		// show status
		lines, err = admin.Do(s, gearman.AdmOptStatus())
		if err != nil {
			log.Fatal(err)
		}
		log.Println("status")
		gearman.PrintLines(lines)
	}
}
