// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package functional

import (
    "fmt"
    "testing"
)

func TestNormal(t *testing.T) {
  s := Slice(Count(), 0, 5)
  ec := newEvenNumberConsumer()
  oc := newOddNumberConsumer()
  if output := MultiConsume(s, new(int), nil, ec, oc); output != nil {
    t.Errorf("Expected MultiConsume to return nil, got %v", output)
  }
  if output := fmt.Sprintf("%v", ec.results); output != "[0 2 4]" {
    t.Errorf("Expected [0 2 4] got %v", output)
  }
  if output := ec.err; output != Done {
    t.Errorf("Expected Done from sub stream, got %v", output)
  }
  if output := fmt.Sprintf("%v", oc.results); output != "[1 3]" {
    t.Errorf("Expected [1 3] got %v", output)
  }
  if output := oc.err; output != Done {
    t.Errorf("Expected Done from sub stream, got %v", output)
  }
}

func TestConsumersEndEarly(t *testing.T) {
  s := &streamCloseChecker{Count(), &simpleCloseChecker{}}
  first5 := func(s Stream) Stream {
    return Slice(s, 0, 5)
  }
  ec := newEvenNumberConsumer()
  oc := newOddNumberConsumer()
  nc := &noNextConsumer{}
  if output := MultiConsume(
      s,
      new(int),
      nil,
      nc,
      ModifyConsumerStream(ec, first5),
      ModifyConsumerStream(oc, first5)); output != nil {
    t.Errorf("Expected MultiConsume to return nil, got %v", output)
  }
  if output := fmt.Sprintf("%v", ec.results); output != "[0 2 4]" {
    t.Errorf("Expected [0 2 4] got %v", output)
  }
  if output := ec.err; output != Done {
    t.Errorf("Expected Done from sub stream, got %v", output)
  }
  if output := fmt.Sprintf("%v", oc.results); output != "[1 3]" {
    t.Errorf("Expected [1 3] got %v", output)
  }
  if output := oc.err; output != Done {
    t.Errorf("Expected Done from sub stream, got %v", output)
  }
  if !nc.completed {
    t.Error("MultiConsume returned before child consumers completed.")
  }
  verifyCloseCalled(t, s)
}

func TestNoConsumers(t *testing.T) {
  s := &streamCloseChecker{CountFrom(7, 1), &simpleCloseChecker{}}
  if output := MultiConsume(s, new(int), nil); output != nil {
    t.Errorf("Expected MultiConsume to return nil, got %v", output)
  }
  verifyCloseCalled(t, s)
}

func TestCloseErrorReturned(t *testing.T) {
  s := &streamCloseChecker{Count(), &simpleCloseChecker{closeError: closeError}}
  if output := MultiConsume(s, new(int), nil); output != closeError {
    t.Errorf("Expected MultiConsume to return closeError, got %v", output)
  }
  verifyCloseCalled(t, s)
}

func TestNoNextConsumer(t *testing.T) {
  s := &streamCloseChecker{CountFrom(7, 1), &simpleCloseChecker{}}
  nc := &noNextConsumer{}
  if output := MultiConsume(s, new(int), nil, nc); output != nil {
    t.Errorf("Expected MultiConsume to return nil, got %v", output)
  }
  if !nc.completed {
    t.Error("MultiConsume returned before child consumers completed.")
  }
  verifyCloseCalled(t, s)
} 

func TestReadPastEndConsumer(t *testing.T) {
  s := Slice(Count(), 0, 5)
  rc1 := &readPastEndConsumer{}
  rc2 := &readPastEndConsumer{}
  if output := MultiConsume(s, new(int), nil, rc1, rc2); output != nil {
    t.Errorf("Expected MultiConsume to return nil, got %v", output)
  }
  if !rc1.completed || !rc2.completed {
    t.Error("MultiConsume returned before child consumers completed.")
  }
}

type filterConsumer struct {
  f Filterer
  results []int
  err error
}

func (fc *filterConsumer) Consume(s Stream) {
  fc.results, fc.err = toIntArray(Filter(fc.f, s))
}

type readPastEndConsumer struct {
  completed bool
}

func (c *readPastEndConsumer) Consume(s Stream) {
  toIntArray(s)
  var x int
  for i := 0; i < 10; i++ {
    s.Next(&x)
  }
  c.completed = true
}

type noNextConsumer struct {
  completed bool
}

func (nc *noNextConsumer) Consume(s Stream) {
  nc.completed = true
}

func newEvenNumberConsumer() *filterConsumer {
  return &filterConsumer{f: NewFilterer(func(ptr interface{}) error {
    p := ptr.(*int)
    if *p % 2 == 0 {
      return nil
    }
    return Skipped
  })}
}

func newOddNumberConsumer() *filterConsumer {
  return &filterConsumer{f: NewFilterer(func(ptr interface{}) error {
    p := ptr.(*int)
    if *p % 2 == 1 {
      return nil
    }
    return Skipped
  })}
}
