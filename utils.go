package gearman

import "fmt"

type FlagChan chan struct{}

var Flag struct{}

func PrintLines(lines [][]byte) {
	for _, line := range lines {
		fmt.Println(string(line))
	}
}
