package gearman

import "github.com/pkg/errors"

var (
	NetworkError  = errors.New("connection corrupt")
	TaskFailError = errors.New("task failed")
)
