package functional

import (
  "errors"
)

// Emitter allows a function to emit values to an associated Stream.
type Emitter interface {

  // EmitPtr returns the pointer supplied to Next of associated Stream.
  // If associated Stream has been closed, EmitPtr returns nil.
  EmitPtr() interface{}

  // Return causes Next of associated Stream to return. Return yields control
  // to the caller of Next blocking until Next on associated Stream is called
  // again or Stream is closed. err is the value that Next should return.
  // err != functional.Done otherwise Return panics.
  Return(err error)
}

// NewGenerator creates a Stream that emits the values from emitting
// function f. When f is through emitting values, it should just return. If
// f gets nil when calling EmitPtr on e it should return immediately as this
// means the Stream was closed.
func NewGenerator(f func(e Emitter)) Stream {
  result := &regularGenerator{emitterStream{ptrCh: make(chan interface{}), errCh: make(chan error)}}
  go func() {
    result.startEmitter()
    f(result)
    result.endEmitter()
  }()
  return result
}

type regularGenerator struct {
  emitterStream
}

func (s *regularGenerator) Return(err error) {
  if err == Done {
    panic("Can't pass functional.Done to Return of Emitter")
  }
  s.emitterStream.Return(err)
}

func (s *regularGenerator) Next(ptr interface{}) error {
  if s.isClosed() {
    return Done
  }
  result := s.emitterStream.Next(ptr)
  if result == Done {
    s.close()
  }
  return result
}

func (s *regularGenerator) Close() error {
  result := s.Next(nil)
  if result == Done {
    return nil
  }
  if result == nil {
    return errors.New("Emitting function did not return on Close.")
  }
  return result
}
