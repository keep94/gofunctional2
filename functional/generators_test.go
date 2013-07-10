// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package functional

import (
    "fmt"
    "testing"
)

func TestNewInfiniteGenerator(t *testing.T) {

  var finished bool
  // fibonacci
  fib := NewGenerator(
      func(e Emitter) {
        a := 0
        b := 1
        for ptr := e.EmitPtr(); ptr != nil; ptr = e.EmitPtr() {
          p := ptr.(*int)
          *p = a
          e.Return(nil)
          a, b = b, a + b
        }
        finished = true
      })
  var results []int
  stream := Slice(fib, 0, 7)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[0 1 1 2 3 5 8]"  {
    t.Errorf("Expected [0 1 1 2 3 5 8] got %v", output)
  }
  if !finished {
    t.Error("Generating function should complete on close.")
  }
  verifyDone(t, stream, new(int), err)
}

func TestNewFiniteGenerator(t *testing.T) {
  var finished bool
  stream := NewGenerator(
      func(e Emitter) {
        values := []int{1, 2, 5}
        for i := range values {
          ptr := e.EmitPtr()
          if ptr == nil {
            break
          }
          *ptr.(*int) = values[i]
          e.Return(nil)
        }
        finished = true
      })
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[1 2 5]" {
    t.Errorf("Expected [1 2 5] got %v", output)
  }
  if !finished {
    t.Error("Generating function should have completed.")
  }
  verifyDone(t, stream, new(int), err)
}

func TestEmptyGenerator(t *testing.T) {
  var finished bool
  stream := NewGenerator(func (e Emitter) { finished = true })
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]" {
    t.Errorf("Expected [] got %v", output)
  }
  if !finished {
    t.Error("Generating function should have completed.")
  }
  verifyDone(t, stream, new(int), err)
}

func TestCloseMayFailClose(t *testing.T) {
  stream := NewGeneratorCloseMayFail(closeFailEmitterFunc)
  closeVerifyResult(t, stream, closeError)
  closeVerifyResult(t, stream, closeError)
}

func TestCloseMayFailNext(t *testing.T) {
  stream := NewGeneratorCloseMayFail(closeFailEmitterFunc)
  var x int
  if err := stream.Next(&x); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
  closeVerifyResult(t, stream, closeError)
}

func TestEmitAllClosed(t *testing.T) {
  s := Count()
  e := fakeEmitter{nil}
  if output := EmitAll(s, e); output != Done {
    t.Errorf("Expected Done, got %v", output)
  }
}

func TestEmitAllSuccess(t *testing.T) {
  s := xrange(0, 10)
  e := fakeEmitter{new(int)}
  if output := EmitAll(s, e); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
}

func closeFailEmitterFunc(e Emitter) error {
  return closeError
}

type fakeEmitter struct {
  ptr interface{}
}

func (e fakeEmitter) EmitPtr() interface{} {
  return e.ptr
}

func (e fakeEmitter) Return(err error) {
}

