package gearman

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdmin_Do(t *testing.T) {
	// show version
	adm := NewAdmin([]string{"localhost:4730"})

	lines, err := adm.Do(AdmOptVersion())
	assert.Nil(t, err, "version return err")
	for _, line := range lines {
		t.Log(string(line))
	}

	lines, err = adm.Do(AdmOptStatus())
	assert.Nil(t, err, "status return err")
	for _, line := range lines {
		t.Log(string(line))
	}

	lines, err = adm.Do(AdmOptWorkers())
	assert.Nil(t, err, "workers return err")
	for _, line := range lines {
		t.Log(string(line))
	}
}
