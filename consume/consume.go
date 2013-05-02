// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

// Package consume provides useful ways to consume streams. All API in this
// package is draft API and may change in incompatible ways.
package consume

import (
  "github.com/keep94/gofunctional2/functional"
  "reflect"
)

// ErrorReportingConsumer is a Consumer that reports if an error was
// encountered consuming a stream.
type ErrorReportingConsumer interface {
  functional.Consumer
  // Returns error if one occurred; otherwise returns nil.
  Error() error
}

// Compose returns an ErrorReportingConsumer
// that sends values it consumes to each one of consumers. The returned
// ErrorReportingConsumer reports an error if any of consumers reports
// an error. ptr is a *T where T values being consumed are temporarily held;
// copier knows how to copy the values of type T being consumed
// (can be nil if simple assignment should be used). If caller passes a slice
// for consumers, no copy is made of it.
func Compose(
    ptr interface{},
    copier functional.Copier,
    consumers ...ErrorReportingConsumer) ErrorReportingConsumer {
  return &compositeConsumer{ptr: ptr, copier: copier, consumers: consumers}
}

// Filter creates a new ErrorReportingConsumer whose Consume method applies
// f to the Stream before passing it onto c.
func Filter(
    c ErrorReportingConsumer, f functional.Filterer) ErrorReportingConsumer {
  return Modify(
      c,
      func(s functional.Stream) functional.Stream {
        return functional.Filter(f, s)
      })
}

// Modify returns a new ErrorReportingConsumer
// that applies f to its Stream and then gives the result to erc. If erc is
// a Consumer of T and f takes a Stream of U and returns a Stream of T, then
// Modify returns a Consumer of U.
func Modify(
    erc ErrorReportingConsumer,
    f func(s functional.Stream) functional.Stream) ErrorReportingConsumer {
  return &modifyConsumer{ErrorReportingConsumer: erc, f: f}
}

// Buffer reads T values from a Stream of T until it either fills up or
// the Stream is exhaused.
type Buffer struct {
  buffer reflect.Value
  addrFunc func(reflect.Value) interface{}
  err error
  idx int
}

// NewBuffer creates a new Buffer. aSlice is a []T used to store values.
func NewBuffer(aSlice interface{}) *Buffer {
  value := reflect.ValueOf(aSlice)
  if value.Kind() != reflect.Slice {
    panic("NewBuffer expects a slice.")
  }
  return &Buffer{buffer: value, addrFunc: forValue}
}

// NewPtrBuffer creates a new Buffer. aSlice is a []*T used to store values.
// Each pointer in aSlice should be non-nil.
func NewPtrBuffer(aSlice interface{}) *Buffer {
  value := reflect.ValueOf(aSlice)
  if value.Kind() != reflect.Slice {
    panic("NewBuffer expects a slice.")
  }
  return &Buffer{buffer: value, addrFunc: forPtr}
}

// Values returns the values gathered from the last Consume call. The number of
// values gathered will not exceed the length of the original slice passed
// to NewBuffer. Returned value is a []T or []*T depending on whether
// NewBuffer or NewPtrBuffer was used to create this instance. Returned
// value remains valid until the next call to Consume.
func (b *Buffer) Values() interface{} {
  return b.buffer.Slice(0, b.idx).Interface()
}

// Error returns any error from last call to Consume.
func (b *Buffer) Error() error {
  return b.err
}

// Consume fetches the entries. s is a Stream of T.
func (b *Buffer) Consume(s functional.Stream) {
  b.consume(s)
  if b.err == functional.Done {
    b.err = nil
  } else {
    s.Close()
  }
}

func (b *Buffer) consume(s functional.Stream) {
  l := b.buffer.Len()
  b.err = nil
  for b.idx = 0; b.idx < l; b.idx++ {
    b.err = s.Next(b.addrFunc(b.buffer.Index(b.idx)))
    if b.err != nil {
      break
    }
  }
}

// PageBuffer reads a page of T values from a stream of T.
type PageBuffer struct {
  buffers [2]Buffer
  addrFunc func(value reflect.Value) interface{}
  desired_page_no int
  page_no int
  is_end bool
}

// NewPageBuffer returns a new PageBuffer instance.
// aSlice is a []T whose length is double that of each page;
// desiredPageNo is the desired 0-based page number. NewPageBuffer panics
// if the length of aSlice is odd.
func NewPageBuffer(aSlice interface{}, desiredPageNo int) *PageBuffer {
  return newPageBuffer(aSlice, desiredPageNo, forValue)
}

// NewPtrPageBuffer returns a new PageBuffer instance.
// aSlice is a []*T whose length is double that of each page;
// desiredPageNo is the desired 0-based page number. NewPageBuffer panics
// if the length of aSlice is odd. Each element of aSlice should be non-nil.
func NewPtrPageBuffer(aSlice interface{}, desiredPageNo int) *PageBuffer {
  return newPageBuffer(aSlice, desiredPageNo, forPtr)
}

func newPageBuffer(
    aSlice interface{},
    desiredPageNo int,
    addrFunc func(reflect.Value) interface{}) *PageBuffer {
  value := reflect.ValueOf(aSlice)
  if value.Kind() != reflect.Slice {
    panic("NewPageBuffer expects a slice.")
  }
  l := value.Len()
  if l % 2 == 1 {
    panic("Slice passed to NewPageBuffer must have even length.")
  }
  if l == 0 {
    panic("Slice passed to NewPageBuffer must have non-zero length.")
  }
  mid := l / 2
  return &PageBuffer{
      buffers: [2]Buffer{
          {buffer: value.Slice(0, mid), addrFunc: addrFunc},
          {buffer: value.Slice(mid, l), addrFunc: addrFunc}},
      addrFunc: addrFunc,
      desired_page_no: desiredPageNo}
}

// Values returns the values of the fetched page as a []T or a []*T depending
// on whether NewPageBuffer or NewPtrPageBuffer was used to create this
// insstance. Returned slice is valid until next call to consume.
func (pb *PageBuffer) Values() interface{} {
  return pb.buffers[pb.page_no % 2].Values()
}

// PageNo returns the 0-based page number of fetched page. Note that this
// returned page number may be less than the desired page number if the
// Stream passed to Consume becomes exhaused.
func (pb *PageBuffer) PageNo() int {
  return pb.page_no
}

// Error returns any error from last call to Consume.
func (pb *PageBuffer) Error() error {
  return pb.buffers[pb.page_no % 2].err
}

// End returns true if last page reached.
func (pb *PageBuffer) End() bool {
  return pb.is_end
}

// Consume fetches the entries. s is a Stream of T.
func (pb *PageBuffer) Consume(s functional.Stream) {
  pb.page_no = 0
  pb.is_end = false
  for {
    buffer := &pb.buffers[pb.page_no % 2]
    buffer.consume(s)
    if buffer.err == functional.Done {
      pb.is_end = true
      if pb.page_no > 0 && buffer.idx == 0 {
        pb.page_no--
      } else {
        buffer.err = nil
      }
      return
    } else if buffer.err != nil || pb.page_no >= pb.desired_page_no {
      // Here we have to test if end is reached
      if buffer.err == nil {
        pb.is_end = s.Next(pb.addrFunc(pb.buffers[(pb.page_no + 1) %2].buffer.Index(0))) == functional.Done
      }
      s.Close()
      return
    }
    pb.page_no++
  }
}

// FirstOnly reads the first value from stream storing it in ptr.
// FirstOnly closes the stream.
// FirstOnly returns emptyError if no values were on stream.
func FirstOnly(stream functional.Stream, emptyError error, ptr interface{}) error {
  err := stream.Next(ptr)
  if err == functional.Done {
    return emptyError
  }
  if err != nil {
    stream.Close()
    return err
  }
  return stream.Close()
}

type compositeConsumer struct {
  ptr interface{}
  copier functional.Copier
  consumers []ErrorReportingConsumer
  closeError error
}

func (c *compositeConsumer) Error() error {
  for _, r := range c.consumers {
    if err := r.Error(); err != nil {
      return err
    }
  }
  return c.closeError
}

func (c *compositeConsumer) Consume(s functional.Stream) {
  consumers := make([]functional.Consumer, len(c.consumers))
  for i := range consumers {
    consumers[i] = c.consumers[i]
  }
  c.closeError = functional.MultiConsume(s, c.ptr, c.copier, consumers...)
}

type modifyConsumer struct {
  ErrorReportingConsumer
  f func(s functional.Stream) functional.Stream
}

func (c *modifyConsumer) Consume(s functional.Stream) {
  c.ErrorReportingConsumer.Consume(c.f(s))
}

func forValue(value reflect.Value) interface{} {
  return value.Addr().Interface()
}

func forPtr(ptrValue reflect.Value) interface{} {
  return ptrValue.Interface()
}
