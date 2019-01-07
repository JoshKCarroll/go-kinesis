package batchproducer

type Event interface {
	String() string
}

var (
	_ Event = (*Error)(nil)
	_ error = (*Error)(nil)
)

type Error struct {
	str string
}

func newError(str string) *Error {
	return &Error{
		str: str,
	}
}

func (e *Error) String() string {
	return e.str
}

func (e *Error) Error() string {
	return e.String()
}
