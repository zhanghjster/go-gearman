package gearman

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransport_ReadWrite(t *testing.T) {
	ts, err := NewTransport("127.0.0.1:4730")
	assert.Nil(t, err, "transport init error")
	var req = newRequestWithType(PtEchoReq)

	var data = "echo data"
	err = req.SetData([]byte(data))
	assert.Nil(t, err, "set echo data err")

	err = ts.Send(req)
	assert.Nil(t, err, "write should be no err")
	resp, err := ts.Read()

	assert.Nil(t, err, "read should be no err")
	assert.Equal(t, PtEchoRes, resp.Type)

	resData, _ := resp.GetData()
	assert.Equal(t, data, string(resData))

}

func BenchmarkTransport_ReadWrite(b *testing.B) {
	ts, _ := NewTransport("127.0.0.1:4730")
	var req = newRequestWithType(PtEchoReq)
	for i := 0; i < b.N; i++ {
		ts.Send(req)
		ts.Read()
	}
}
