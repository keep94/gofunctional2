package consume

import (
  "errors"
  "github.com/keep94/gofunctional2/functional"
  "testing"
)

var (
  emptyError = errors.New("stream_util: Empty.")
  otherError = errors.New("stream_util: Other.")
  consumerError = errors.New("stream_util: consumer error.")
  closeError = errors.New("stream_util: close error.")
)

func TestPtrBuffer(t *testing.T) {
  stream := &closeChecker{Stream: functional.Count()}
  b := newPtrBuffer(5)
  b.Consume(stream)
  verifyClosed(t, stream)
  verifyPtrFetched(t, b, 0, 5)
}

func TestBufferSameSize(t *testing.T) {
  stream := &closeChecker{Stream: functional.Slice(functional.Count(), 0, 5)}
  b := NewBuffer(make([]int, 5))
  b.Consume(stream)
  verifyClosed(t, stream)
  verifyFetched(t, b, 0, 5)
}

func TestBufferSmall(t *testing.T) {
  stream := &closeChecker{Stream: functional.Slice(functional.Count(), 0, 6)}
  b := NewBuffer(make([]int, 5))
  b.Consume(stream)
  verifyClosed(t, stream)
  verifyFetched(t, b, 0, 5)
}

func TestBufferBig(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 4)
  b := NewBuffer(make([]int, 5))
  b.Consume(stream)
  verifyFetched(t, b, 0, 4)
}

func TestBufferError(t *testing.T) {
  stream := &closeChecker{Stream: errorStream{otherError}}
  b := NewBuffer(make([]int, 5))
  b.Consume(stream)
  if err := b.Error(); err != otherError {
    t.Errorf("Expected error otherError, got %v", err)
  }
}

func TestPtrPageBuffer(t *testing.T) {
  stream := &closeChecker{Stream: functional.Count()}
  pb := newPtrPageBuffer(6, 0)
  pb.Consume(stream)
  verifyClosed(t, stream)
  verifyPtrPageFetched(t, pb, 0, 3, 0, false)
}

func TestPageBufferFirstPage(t *testing.T) {
  stream := &closeChecker{Stream: functional.Count()}
  pb := NewPageBuffer(make([]int, 6), 0)
  pb.Consume(stream)
  verifyClosed(t, stream)
  verifyPageFetched(t, pb, 0, 3, 0, false)
}

func TestPageBufferSecondPage(t *testing.T) {
  stream := &closeChecker{Stream: functional.Count()}
  pb := NewPageBuffer(make([]int, 6), 1)
  pb.Consume(stream)
  verifyClosed(t, stream)
  verifyPageFetched(t, pb, 3, 6, 1, false)
}

func TestPageBufferThirdPage(t *testing.T) {
  stream := &closeChecker{Stream: functional.Count()}
  pb := NewPageBuffer(make([]int, 6), 2)
  pb.Consume(stream)
  verifyClosed(t, stream)
  verifyPageFetched(t, pb, 6, 9, 2, false)
}

func TestPageBufferNegativePage(t *testing.T) {
  stream := &closeChecker{Stream: functional.Count()}
  pb := NewPageBuffer(make([]int, 6), -1)
  pb.Consume(stream)
  verifyClosed(t, stream)
  verifyPageFetched(t, pb, 0, 3, 0, false)
}

func TestPageBufferParitalThird(t *testing.T) {
  stream := &closeChecker{
      Stream: functional.Slice(functional.Count(), 0, 7)}
  pb := NewPageBuffer(make([]int, 6), 2)
  pb.Consume(stream)
  verifyPageFetched(t, pb, 6, 7, 2, true)
}

func TestPageBufferParitalThirdToHigh(t *testing.T) {
  stream := &closeChecker{
      Stream: functional.Slice(functional.Count(), 0, 7)}
  pb := NewPageBuffer(make([]int, 6), 3)
  pb.Consume(stream)
  verifyPageFetched(t, pb, 6, 7, 2, true)
}

func TestPageBufferEmptyThird(t *testing.T) {
  stream := &closeChecker{
      Stream: functional.Slice(functional.Count(), 0, 6)}
  pb := NewPageBuffer(make([]int, 6), 2)
  pb.Consume(stream)
  verifyPageFetched(t, pb, 3, 6, 1, true)
}

func TestPageBufferEmptyThirdTooHigh(t *testing.T) {
  stream := &closeChecker{
      Stream: functional.Slice(functional.Count(), 0, 6)}
  pb := NewPageBuffer(make([]int, 6), 3)
  pb.Consume(stream)
  verifyPageFetched(t, pb, 3, 6, 1, true)
}

func TestPageBufferFullSecond(t *testing.T) {
  stream := &closeChecker{
      Stream: functional.Slice(functional.Count(), 0, 6)}
  pb := NewPageBuffer(make([]int, 6), 1)
  pb.Consume(stream)
  verifyClosed(t, stream)
  verifyPageFetched(t, pb, 3, 6, 1, true)
}

func TestPageBufferParitalFirst(t *testing.T) {
  stream := &closeChecker{
      Stream: functional.Slice(functional.Count(), 0, 1)}
  pb := NewPageBuffer(make([]int, 6), 0)
  pb.Consume(stream)
  verifyPageFetched(t, pb, 0, 1, 0, true)
}

func TestPageBufferEmpty(t *testing.T) {
  stream := &closeChecker{
      Stream: functional.NilStream()}
  pb := NewPageBuffer(make([]int, 6), 0)
  pb.Consume(stream)
  verifyPageFetched(t, pb, 0, 0, 0, true)
}

func TestPageBufferEmptyHigh(t *testing.T) {
  stream := &closeChecker{
      Stream: functional.NilStream()}
  pb := NewPageBuffer(make([]int, 6), 1)
  pb.Consume(stream)
  verifyPageFetched(t, pb, 0, 0, 0, true)
}

func TestPageBufferEmptyLow(t *testing.T) {
  stream := &closeChecker{
      Stream: functional.NilStream()}
  pb := NewPageBuffer(make([]int, 6), -1)
  pb.Consume(stream)
  verifyPageFetched(t, pb, 0, 0, 0, true)
}

func TestPageBufferError(t *testing.T) {
  stream := &closeChecker{Stream: errorStream{otherError}}
  b := NewPageBuffer(make([]int, 6), 0)
  b.Consume(stream)
  if err := b.Error(); err != otherError {
    t.Errorf("Expected error otherError, got %v", err)
  }
}

func TestFirstOnly(t *testing.T) {
  stream := &closeChecker{Stream: functional.CountFrom(3, 1)}
  var value int
  if output := FirstOnly(stream, emptyError, &value); output != nil {
    t.Errorf("Got error fetching first value, %v", output)
  }
  if value != 3 {
    t.Errorf("Expected 3, got %v", value)
  }
  verifyClosed(t, stream)
}

func TestFirstOnlyEmpty(t *testing.T) {
  stream := functional.NilStream()
  var value int
  if output := FirstOnly(stream, emptyError, &value); output != emptyError {
    t.Errorf("Expected emptyError, got %v", output)
  }
}

func TestFirstOnlyError(t *testing.T) {
  stream := &closeChecker{Stream: errorStream{otherError}}
  var value int
  if output := FirstOnly(stream, emptyError, &value); output != otherError {
    t.Errorf("Expected emptyError, got %v", output)
  }
  verifyClosed(t, stream)
}

func TestCompose(t *testing.T) {
  consumer1 := errorReportingConsumerForTesting{}
  consumer2 := errorReportingConsumerForTesting{}
  errorReportingConsumer := Compose(
      new(int), nil, &consumer1, &consumer2)
  errorReportingConsumer.Consume(
      closeErrorStream{functional.Slice(functional.Count(), 0, 5)})
  if err := errorReportingConsumer.Error(); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
  if output := consumer1.count; output != 5 {
    t.Errorf("Expected 5, got %v", output)
  }
  if output := consumer2.count; output != 5 {
    t.Errorf("Expected 5, got %v", output)
  }
}

func TestCompose2(t *testing.T) {
  consumer1 := errorReportingConsumerForTesting{}
  consumer2 := errorReportingConsumerForTesting{e: consumerError}
  errorReportingConsumer := Compose(
      new(int),
      nil,
      &consumer1,
      Modify(
          &consumer2,
          func (s functional.Stream) functional.Stream {
            return functional.Slice(s, 0, 2)
          }))
  errorReportingConsumer.Consume(
      closeErrorStream{functional.Slice(functional.Count(), 0, 5)})
  if err := errorReportingConsumer.Error(); err != consumerError {
    t.Errorf("Expected consumerError, got %v", err)
  }
  if output := consumer1.count; output != 5 {
    t.Errorf("Expected 5, got %v", output)
  }
  if output := consumer2.count; output != 2 {
    t.Errorf("Expected 2, got %v", output)
  }
}

func verifyFetched(t *testing.T, b *Buffer, start int, end int) {
  if err := b.Error(); err != nil {
    t.Errorf("Got error fetching values, %v", err)
    return
  }
  verifyValues(t, b.Values().([]int), start, end)
}

func verifyPtrFetched(t *testing.T, b *Buffer, start int, end int) {
  if err := b.Error(); err != nil {
    t.Errorf("Got error fetching values, %v", err)
    return
  }
  verifyPtrValues(t, b.Values().([]*int), start, end)
}

func verifyPageFetched(t *testing.T, pb *PageBuffer, start int, end int, page_no int, is_end bool) {
  if err := pb.Error(); err != nil {
    t.Errorf("Got error fetching page values, %v", err)
    return
  }
  verifyValues(t, pb.Values().([]int), start, end)
  if output := pb.PageNo(); output != page_no {
    t.Errorf("Expected page %v, got %v", page_no, output)
  }
  if output := pb.End(); output != is_end {
    t.Errorf("For end, expected %v, got %v", is_end, output)
  }
}

func verifyPtrPageFetched(t *testing.T, pb *PageBuffer, start int, end int, page_no int, is_end bool) {
  if err := pb.Error(); err != nil {
    t.Errorf("Got error fetching page values, %v", err)
    return
  }
  verifyPtrValues(t, pb.Values().([]*int), start, end)
  if output := pb.PageNo(); output != page_no {
    t.Errorf("Expected page %v, got %v", page_no, output)
  }
  if output := pb.End(); output != is_end {
    t.Errorf("For end, expected %v, got %v", is_end, output)
  }
}

func verifyValues(t *testing.T, values []int, start int, end int) {
  if output := len(values); output != end - start {
    t.Errorf("Expected entry array to be %v, got %v", end - start, output)
    return
  }
  for i := start; i < end; i++ {
    if output := values[i - start]; output != i {
      t.Errorf("Expected %v, got %v", i, output)
    }
  }
}

func verifyPtrValues(t *testing.T, values []*int, start int, end int) {
  if output := len(values); output != end - start {
    t.Errorf("Expected entry array to be %v, got %v", end - start, output)
    return
  }
  for i := start; i < end; i++ {
    if output := *values[i - start]; output != i {
      t.Errorf("Expected %v, got %v", i, output)
    }
  }
}

func verifyClosed(t *testing.T, c *closeChecker) {
  if !c.closed {
    t.Error("Stream not closed.")
  }
}

type closeChecker struct {
  functional.Stream
  closed bool
}

func (c *closeChecker) Close() error {
  c.closed = true
  return c.Stream.Close()
}

type errorStream struct {
  err error
}

func (e errorStream) Next(ptr interface{}) error {
  return e.err
}

func (e errorStream) Close() error {
  return nil
}

type errorReportingConsumerForTesting struct {
  count int
  e error
}

func (c *errorReportingConsumerForTesting) Consume(s functional.Stream) {
  var x int
  for s.Next(&x) != functional.Done {
    c.count++
  }
}

func (c *errorReportingConsumerForTesting) Error() error {
  return c.e
}

type closeErrorStream struct {
  functional.Stream
}

func (c closeErrorStream) Close() error {
  return closeError
}

func newPtrBuffer(size int) *Buffer {
  array := make([]*int, size)
  for i := range array {
    array[i] = new(int)
  }
  return NewPtrBuffer(array)
}

func newPtrPageBuffer(size, desiredPageNo int) *PageBuffer {
  array := make([]*int, size)
  for i := range array {
    array[i] = new(int)
  }
  return NewPtrPageBuffer(array, desiredPageNo)
}
