package beanpod

import (
	"github.com/kr/beanstalk"
)

var (
	ErrBadFormat  = beanstalk.ErrBadFormat
	ErrBuried     = beanstalk.ErrBuried
	ErrDeadline   = beanstalk.ErrDeadline
	ErrDraining   = beanstalk.ErrDraining
	ErrInternal   = beanstalk.ErrInternal
	ErrJobTooBig  = beanstalk.ErrJobTooBig
	ErrNoCRLF     = beanstalk.ErrNoCRLF
	ErrNotFound   = beanstalk.ErrNotFound
	ErrNotIgnored = beanstalk.ErrNotIgnored
	ErrOOM        = beanstalk.ErrOOM
	ErrTimeout    = beanstalk.ErrTimeout
	ErrUnknown    = beanstalk.ErrUnknown
	ErrEmpty      = beanstalk.ErrEmpty
	ErrBadChar    = beanstalk.ErrBadChar
	ErrTooLong    = beanstalk.ErrTooLong
)

func unwrap(err error) error {
	if connErr, ok := err.(beanstalk.ConnError); ok {
		return connErr.Err
	} else if nameErr, ok := err.(beanstalk.NameError); ok {
		return nameErr.Err
	}
	return err
}
