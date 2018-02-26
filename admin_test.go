package gearman

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdmin_Do(t *testing.T) {
	var server = "localhost:4730"
	// show version
	adm := NewAdmin([]string{server})

	lines, err := adm.Do(server, AdmOptVersion())
	assert.Nil(t, err, "version return err")
	for _, line := range lines {
		t.Log(string(line))
	}

	lines, err = adm.Do(server, AdmOptStatus())
	assert.Nil(t, err, "status return err")
	for _, line := range lines {
		t.Log(string(line))
	}

	lines, err = adm.Do(server, AdmOptWorkers())
	assert.Nil(t, err, "workers return err")
	for _, line := range lines {
		t.Log(string(line))
	}

	// _, err = adm.Do(server, AdmOptShutdownGraceful())
	// assert.Nil(t, err, "shutdown return err")
}
