// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

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
  return NewGeneratorCloseMayFail(func(e Emitter) error {
    f(e)
    return nil
  })
}

// NewGeneratorCloseMayFail creates a Stream that emits the values from
// emitting function f. When f is through emitting values, it should perform
// any necessary cleanup and return any error from the cleanup.
// If f gets nil when calling EmitPtr on e it should immediately perform
// cleanup returning any error from the cleanup as this means the Stream
// was closed. The Close() method on returned Stream reports any non-nil error
// f returns to the caller.
// This function is draft API and may change in incompatible ways.
func NewGeneratorCloseMayFail(f func(e Emitter) error) Stream {
  result := &regularGenerator{emitterStream: emitterStream{ptrCh: make(chan interface{}), errCh: make(chan error)}}
  go func() {
    var err error
    defer func() {
      result.endEmitter(err)
    }()
    result.startEmitter()
    err = f(result)
  }()
  return result
}

// EmitAll emits all of Stream s to Emitter e. On success, returns nil.
// If the Stream for e becomes closed, EmitAll closes s and returns Done.
// If there was an error closing s, it returns that error.
func EmitAll(s Stream, e Emitter) error {
  for ptr := e.EmitPtr(); ptr != nil; ptr = e.EmitPtr() {
    err := s.Next(ptr)
    if err == Done {
      return nil
    }
    e.Return(err)
  }
  return finish(s.Close())
}

type regularGenerator struct {
  emitterStream
  closeResult error
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
    s.closeResult = <-s.errCh
    s.close()
    return finish(s.closeResult)
  }
  return result
}

func (s *regularGenerator) Close() error {
  if s.isClosed() {
    return s.closeResult
  }
  result := s.Next(nil)
  if !s.isClosed() {
    return errors.New("Emitting function did not return on Close.")
  }
  if result == Done {
    return nil
  }
  return result
}
