package gearman

import "fmt"

type FlagChan chan struct{}

var Flag struct{}

func PrintLines(lines []string) {
	for _, line := range lines {
		fmt.Println(line)
	}
}
