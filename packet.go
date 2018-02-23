package gearman

import (
	"time"

	"encoding/binary"

	"bytes"

	"net"

	"github.com/pkg/errors"
)

type PacketType uint32

const (
	PtCanDo PacketType = iota + 1
	PtCantDo
	PtResetAbilities
	PtPreSleep
	_
	PtNoop
	PtSubmitJob
	PtJobCreated
	PtGrabJob
	PtNoJob
	PtJobAssign
	PtWorkStatus
	PtWorkComplete
	PtWorkFail
	PtGetStatus
	PtEchoReq
	PtEchoRes
	PtSubmitJobBg
	PtError
	PtStatusRes
	PtSubmitJobHigh
	PtSetClientId
	PtCanDoTimeout
	PtAllYours
	PtWorkException
	PtOptionReq
	PtOptionRes
	PtWorkData
	PtWorkWarning
	PtGrabJobUnique
	PtJobAssignUnique
	PtSubmitJobHighBg
	PtSubmitJobLow
	PtSubmitJobLowBg
	PtSubmitJobSched
	PtSubmitJobEpoch
	PtSubmitReduceJob
	PtSubmitReduceJobBg
	PtGrabJobAll
	PtJobAssignAll
	PtGetStatusUnique
	PtStatusResUnique
	PtAdminWorkers
	PtAdminClients
	PtAdminStatus
	PtAdminMaxQueue
	PtAdminShutdown
	PtAdminVersion
	PtAdminResp
)

var AdminCmdString = map[PacketType]string{
	PtAdminWorkers:  "workers",
	PtAdminClients:  "clients",
	PtAdminStatus:   "status",
	PtAdminMaxQueue: "maxqueue",
	PtAdminShutdown: "shutdown",
	PtAdminVersion:  "version",
	PtAdminResp:     "adminResp", // for read
}

const HeaderSize = 12

type ArgName int

const (
	ArgHandle ArgName = iota
	ArgFuncName
	ArgUniqueId
	ArgReducer
	ArgData
	ArgErrCode
	ArgErrText
	ArgMinute
	ArgHour
	ArgDayOfMonth
	ArgMonth
	ArgDayOfWeek
	ArgEpoch
	ArgConnOption
	ArgPercentNumerator
	ArgPercentDenominator
	ArgWorkerId
	ArgKnowStatus
	ArgRunningStatus
	ArgWaitingClientsNum
	ArgTimeout
	ArgGraceful
	ArgMaxQueue
)

var packetArgs = map[PacketType][]ArgName{
	PtEchoReq:           {ArgData},
	PtEchoRes:           {ArgData},
	PtError:             {ArgErrCode, ArgErrText},
	PtSubmitJobLow:      {ArgFuncName, ArgUniqueId, ArgData},
	PtSubmitJob:         {ArgFuncName, ArgUniqueId, ArgData},
	PtSubmitJobHigh:     {ArgFuncName, ArgUniqueId, ArgData},
	PtSubmitJobLowBg:    {ArgFuncName, ArgUniqueId, ArgData},
	PtSubmitJobBg:       {ArgFuncName, ArgUniqueId, ArgData},
	PtSubmitJobHighBg:   {ArgFuncName, ArgUniqueId, ArgData},
	PtSubmitReduceJob:   {ArgFuncName, ArgUniqueId, ArgReducer, ArgData},
	PtSubmitReduceJobBg: {ArgFuncName, ArgUniqueId, ArgReducer, ArgData},
	PtSubmitJobSched:    {ArgFuncName, ArgUniqueId, ArgMinute, ArgHour, ArgDayOfMonth, ArgMonth, ArgDayOfWeek, ArgData},
	PtSubmitJobEpoch:    {ArgFuncName, ArgUniqueId, ArgEpoch, ArgData},
	PtGetStatus:         {ArgHandle},
	PtGetStatusUnique:   {ArgUniqueId},
	PtOptionReq:         {ArgConnOption},
	PtJobCreated:        {ArgHandle},
	PtWorkData:          {ArgHandle, ArgData},
	PtWorkWarning:       {ArgHandle, ArgData},
	PtWorkStatus:        {ArgHandle, ArgPercentNumerator, ArgPercentDenominator},
	PtWorkComplete:      {ArgHandle, ArgData},
	PtWorkFail:          {ArgHandle},
	PtWorkException:     {ArgHandle, ArgData},
	PtStatusRes:         {ArgHandle, ArgKnowStatus, ArgRunningStatus, ArgPercentNumerator, ArgPercentDenominator},
	PtStatusResUnique:   {ArgHandle, ArgKnowStatus, ArgRunningStatus, ArgPercentNumerator, ArgPercentDenominator, ArgWaitingClientsNum},
	PtOptionRes:         {ArgConnOption},
	PtCanDo:             {ArgHandle, ArgData},
	PtCanDoTimeout:      {ArgFuncName, ArgTimeout},
	PtCantDo:            {ArgFuncName},
	PtResetAbilities:    {},
	PtPreSleep:          {},
	PtGrabJob:           {},
	PtGrabJobUnique:     {},
	PtGrabJobAll:        {},
	PtSetClientId:       {ArgWorkerId},
	PtAllYours:          {},
	PtNoop:              {},
	PtNoJob:             {},
	PtJobAssign:         {ArgHandle, ArgFuncName, ArgData},
	PtJobAssignUnique:   {ArgHandle, ArgFuncName, ArgUniqueId, ArgData},
	PtJobAssignAll:      {ArgHandle, ArgFuncName, ArgUniqueId, ArgReducer, ArgData},
	PtAdminShutdown:     {ArgGraceful},
	PtAdminMaxQueue:     {ArgFuncName, ArgMaxQueue},
}

var ArgIndex = argIndex()

func argIndex() map[PacketType]map[ArgName]int {
	mp := make(map[PacketType]map[ArgName]int)
	for pt, args := range packetArgs {
		mp[pt] = make(map[ArgName]int)
		for i, name := range args {
			mp[pt][name] = i
		}
	}
	return mp
}

var (
	ErrArgNotSupported = errors.New("arg not supported")
)

type Packet struct {
	Type PacketType
	peer *TransportPeer
	args [][]byte
}

func (p *Packet) getArg(name ArgName) (v []byte, err error) {
	if mp, ok := ArgIndex[p.Type]; ok && len(mp) > 0 {
		if index, ok := mp[name]; ok && len(p.args) >= index {
			return p.args[index], nil
		}
	}

	return nil, ErrArgNotSupported
}

func (p *Packet) setArg(name ArgName, v []byte) error {
	if mp, ok := ArgIndex[p.Type]; ok && len(mp) > 0 {
		if len(mp) > 0 && p.args == nil {
			p.args = make([][]byte, len(mp))
		}
	}

	return ErrArgNotSupported
}

func (p *Packet) SetType(tp PacketType) {
	p.Type = tp
}
func (p *Packet) SetPeer(peer *TransportPeer) {
	p.peer = peer
}

func (p *Packet) SetReducer(name string) error {
	return p.setArg(ArgReducer, []byte(name))
}
func (p *Packet) GetReducer() (string, error) {
	arg, err := p.getArg(ArgReducer)
	return p.argByteToString(arg), err
}
func (p *Packet) SetSchedule(t time.Time) error {
	var args = map[ArgName]int{
		ArgMinute:     t.Minute(),
		ArgHour:       t.Hour(),
		ArgDayOfMonth: t.Day(),
		ArgMonth:      int(t.Month()),
		ArgDayOfWeek:  int(t.Weekday()),
	}
	for k, v := range args {
		if err := p.setIntNArg(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (p *Packet) SetEpoch(t int64) error {
	return p.setIntNArg(ArgEpoch, t)
}

func (p *Packet) SetConnOption(name string) error {
	return p.setArg(ArgConnOption, []byte(name))
}
func (p *Packet) GetConnOption() (string, error) {
	arg, err := p.getArg(ArgConnOption)
	return p.argByteToString(arg), err
}

func (p *Packet) GetFuncName() (string, error) {
	arg, err := p.getArg(ArgFuncName)
	return p.argByteToString(arg), err
}

func (p *Packet) SetFuncName(name string) error {
	return p.setArg(ArgFuncName, []byte(name))
}

func (p *Packet) GetData() ([]byte, error) {
	return p.getArg(ArgData)
}

func (p *Packet) SetData(data []byte) error {
	return p.setArg(ArgData, data)
}

func (p *Packet) GetHandle() (string, error) {
	arg, err := p.getArg(ArgHandle)
	return p.argByteToString(arg), err
}

func (p *Packet) SetHandle(handle string) error {
	return p.setArg(ArgHandle, []byte(handle))
}

func (p *Packet) GetUniqueId() (string, error) {
	arg, err := p.getArg(ArgUniqueId)
	return p.argByteToString(arg), err
}

func (p *Packet) SetUniqueId(id string) error {
	return p.setArg(ArgUniqueId, []byte(id))
}

func (p *Packet) GetStatusKnow() (bool, error) {
	arg, err := p.getArg(ArgKnowStatus)
	if err != nil {
		return false, err
	}
	return binary.BigEndian.Uint32(arg) == 1, nil
}

func (p *Packet) GetStatusRunning() (bool, error) {
	arg, err := p.getArg(ArgRunningStatus)
	if err != nil {
		return false, err
	}
	return binary.BigEndian.Uint32(arg) == 1, nil
}

func (p *Packet) GetPercentNumerator() (uint32, error) {
	arg, err := p.getArg(ArgPercentNumerator)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(arg), nil
}

func (p *Packet) GetPercentDenominator() (uint32, error) {
	arg, err := p.getArg(ArgPercentDenominator)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(arg), nil
}

func (p *Packet) SetPercent(n, d uint32) error {
	if err := p.setIntNArg(ArgPercentNumerator, n); err != nil {
		return err
	}

	return p.setIntNArg(ArgPercentDenominator, d)
}

func (p *Packet) GetWaitingClientNum() (uint32, error) {
	arg, err := p.getArg(ArgWaitingClientsNum)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(arg), nil
}

func (p *Packet) SetCanDoTimeout(n int) error {
	return p.setIntNArg(ArgTimeout, n)
}

func (p *Packet) SetGraceful() error {
	return p.setArg(ArgGraceful, []byte("graceful"))
}

func (p *Packet) SetMaxQueue(queue string) error {
	return p.setArg(ArgMaxQueue, []byte(queue))
}

func (p *Packet) argByteToString(arg []byte) string {
	if arg != nil {
		return string(arg)
	} else {
		return ""
	}
}

func (p *Packet) setIntNArg(name ArgName, n interface{}) error {
	var b = new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, n)
	return p.setArg(name, b.Bytes())
}

func (p *Packet) IsAdmin() bool {
	_, ok := AdminCmdString[p.Type]
	return ok
}

func (p *Packet) AdminCmdString() string {
	return AdminCmdString[p.Type]
}

func (p *Packet) ArgsBytes() [][]byte {
	return p.args
}

type Response struct {
	Packet
}

type Request struct {
	Packet

	Timeout <-chan time.Time

	remote    net.Addr // asyncSend to picked server
	broadcast bool     // asyncSend to all server

	resCh chan interface{} // for conn asyncSend result
}

type ResponseHandler func(resp *Response)
