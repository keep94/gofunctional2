// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package functional

import (
    "errors"
    "fmt"
    "io"
    "strings"
    "testing"
)

var (
  scanError = errors.New("error scanning.")
  closeError = errors.New("error closing.")
  filterError = errors.New("filter error.")
  mapError = errors.New("map error.")
  includeFilterer = errorFilterer{nil}
  skipFilterer = errorFilterer{Skipped}
  errFilterer = errorFilterer{filterError}
  idMapper = errorMapper{nil}
  skipMapper = errorMapper{Skipped}
  errMapper = errorMapper{mapError}
  int64Plus1 = NewMapper(
      func (srcPtr interface{}, destPtr interface{}) error {
        p := srcPtr.(*int64)
        q := destPtr.(*int64)
        *q = (*p) + 1
        return nil
      })
  doubleInt32Int64 = NewMapper(
      func (srcPtr interface{}, destPtr interface{}) error {
        p := srcPtr.(*int32)
        q := destPtr.(*int64)
        *q = 2 * int64(*p)
        return nil
      })
  squareIntInt32 = NewMapper(
      func (srcPtr interface{}, destPtr interface{}) error {
        p := srcPtr.(*int)
        q := destPtr.(*int32)
        *q = int32(*p) * int32(*p)
        return nil
  })
)

func TestFilterAndMap(t *testing.T) {
  s := xrange(5, 15)
  f := NewFilterer(func(ptr interface{}) error {
    p := ptr.(*int)
    if *p % 2 == 0 {
      return nil
    }
    return Skipped
  })
  m := NewMapper(func(srcPtr interface{}, destPtr interface{}) error {
    s := srcPtr.(*int)
    d := destPtr.(*int32)
    *d = int32((*s) * (*s))
    return nil
  })
  stream := Map(m, Filter(f, s), new(int))
  results, err := toInt32Array(stream)
  if output := fmt.Sprintf("%v", results); output != "[36 64 100 144 196]"  {
    t.Errorf("Expected [36 64 100 144 196] got %v", output)
  }
  verifyDone(t, stream, new(int32), err)
}

func TestCombineFilterMap(t *testing.T) {
  s := xrange(5, 15)
  m := NewMapper(func(srcPtr interface{}, destPtr interface{}) error {
    s := srcPtr.(*int)
    d := destPtr.(*int32)
    if *s % 2 != 0 {
      return Skipped
    }
    *d = int32((*s) * (*s))
    return nil
  })
  stream := Map(doubleInt32Int64, Map(m, s, new(int)), new(int32))
  results, err := toInt64Array(stream)
  if output := fmt.Sprintf("%v", results); output != "[72 128 200 288 392]"  {
    t.Errorf("Expected [64 128 200 288 392] got %v", output)
  }
  verifyDone(t, stream, new(int64), err)
}

func TestNoFilterInFilter(t *testing.T) {
  s := Filter(greaterThan(5), Filter(lessThan(8), xrange(0, 10)))
  _, filterInFilter := s.(*filterStream).Stream.(*filterStream)
  if filterInFilter {
    t.Error("Got a filter within a filter.")
  }
}

func TestNestedFilter(t *testing.T) {
  stream := Filter(greaterThan(5), Filter(lessThan(8), xrange(0, 10)))
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[6 7]"  {
    t.Errorf("Expected [6 7] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestNoMapInMap(t *testing.T) {
  s := Map(squareIntInt32, xrange(3, 6), new(int))
  s = Map(doubleInt32Int64, s, new(int32))
  _, mapInMap := s.(*mapStream).Stream.(*mapStream)
  if mapInMap {
    t.Error("Got a map within a map.")
  }
}

func TestNestedMap(t *testing.T) {
  s := Map(squareIntInt32, xrange(3, 6), new(int))
  stream := Map(doubleInt32Int64, s, new(int32))
  ms := stream.(*mapStream)
  _, ok := ms.mapper.(fastCompositeMapper)
  if !ok {
    t.Error("Nested Mappes Stream does not contain a fast composite mapper")
  }
  results, err := toInt64Array(stream)
  if output := fmt.Sprintf("%v", results); output != "[18 32 50]"  {
    t.Errorf("Expected [18 32 50] got %v", output)
  }
  verifyDone(t, stream, new(int64), err)
}

func TestNestedMapWithCompositeMapper(t *testing.T) {
  cm := Compose(doubleInt32Int64, squareIntInt32, func() interface{} { return new(int32) })
  stream := Map(cm, xrange(3, 6), new(int))
  ms := stream.(*mapStream)
  _, ok := ms.mapper.(fastCompositeMapper)
  if !ok {
    t.Error("Nested Mapper Stream does not contain a fast composite mapper")
  }
  results, err := toInt64Array(stream)
  if output := fmt.Sprintf("%v", results); output != "[18 32 50]"  {
    t.Errorf("Expected [18 32 50] got %v", output)
  }
  verifyDone(t, stream, new(int64), err)
}

func TestNestedMapWithFastCompositeMapper(t *testing.T) {
  fcm := FastCompose(doubleInt32Int64, squareIntInt32, new(int32))
  stream := Map(fcm, xrange(3, 6), new(int))
  ms := stream.(*mapStream)
  _, ok := ms.mapper.(fastCompositeMapper)
  if !ok {
    t.Error("Nested Mappes Stream does not contain a fast composite mapper")
  }
  results, err := toInt64Array(stream)
  if output := fmt.Sprintf("%v", results); output != "[18 32 50]"  {
    t.Errorf("Expected [18 32 50] got %v", output)
  }
  verifyDone(t, stream, new(int64), err)
}

func TestSliceNoEnd(t *testing.T) {
  s := xrange(5, 13)
  stream := Slice(s, 5, -1)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[10 11 12]"  {
    t.Errorf("Expected [10 11 12] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestSliceWithEnd(t *testing.T) {
  s := &streamCloseChecker{xrange(5, 13), &simpleCloseChecker{}}
  stream := Slice(s, 2, 4)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[7 8]"  {
    t.Errorf("Expected [7 8] got %v", output)
  }
  verifyCloseCalled(t, s)
  verifyDone(t, stream, new(int), err)
}

func TestSliceWithEnd2(t *testing.T) {
  s := &streamCloseChecker{xrange(5, 13), &simpleCloseChecker{}}
  stream := Slice(s, 0, 2)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[5 6]"  {
    t.Errorf("Expected [5 6] got %v", output)
  }
  verifyCloseCalled(t, s)
  verifyDone(t, stream, new(int), err)
}

func TestZeroSlice(t *testing.T) {
  s := &streamCloseChecker{xrange(5, 13), &simpleCloseChecker{}}
  stream := Slice(s, 2, 2)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  verifyCloseCalled(t, s)
  verifyDone(t, stream, new(int), err)
}

func TestSliceStartTooBig(t *testing.T) {
  s := xrange(5, 13)
  stream := Slice(s, 20, 30)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestSliceEndTooBig(t *testing.T) {
  s := xrange(5, 13)
  stream := Slice(s, 7, 10)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[12]"  {
    t.Errorf("Expected [12] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestSliceStartBiggerThanEnd(t *testing.T) {
  s := &streamCloseChecker{xrange(5, 13), &simpleCloseChecker{}}
  stream := Slice(s, 4, 3)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  verifyCloseCalled(t, s)
  verifyDone(t, stream, new(int), err)
}

func TestSliceNextPropagateClose(t *testing.T) {
  s := &streamCloseChecker{Count(), &simpleCloseChecker{closeError: closeError}}
  stream := Slice(s, 7, 10)
  if _ ,err := toIntArray(stream); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
}
  
func TestCountFrom(t *testing.T) {
  stream := Slice(CountFrom(5, 2), 1, 3)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[7 9]"  {
    t.Errorf("Expected [7 9] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestReadRows(t *testing.T) {
  rows := &rowsCloseChecker{
      &fakeRows{ids: []int {3, 4}, names: []string{"foo", "bar"}},
      &simpleCloseChecker{noDupClose: true}}
  stream := ReadRows(rows)
  results, err := toIntAndStringArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[{3 foo} {4 bar}]"  {
    t.Errorf("Expected [{3 foo} {4 bar}] got %v", output)
  }
  verifyCloseCalled(t, rows)
  verifyDone(t, stream, new(intAndString), err)
} 

func TestReadRowsNoImplCloser(t *testing.T) {
  rows := &fakeRows{ids: []int {3, 4}, names: []string{"foo", "bar"}}
  stream := ReadRows(rows)
  results, err := toIntAndStringArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[{3 foo} {4 bar}]"  {
    t.Errorf("Expected [{3 foo} {4 bar}] got %v", output)
  }
  verifyDone(t, stream, new(intAndString), err)
} 

func TestReadRowsEmpty(t *testing.T) {
  rows := &rowsCloseChecker{
      &fakeRows{}, &simpleCloseChecker{noDupClose: true}}
  stream := ReadRows(rows)
  results, err := toIntAndStringArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  verifyCloseCalled(t, rows)
  verifyDone(t, stream, new(intAndString), err)
} 

func TestReadRowsError(t *testing.T) {
  rows := fakeRowsError{}
  s := ReadRows(rows)
  var result intAndString
  if err := s.Next(&result); err == nil || err == Done {
    t.Error("Expected error reading rows.")
  }
  // Close stream after examining error
  s.Close()
}

func TestReadRowsNextPropagateClose(t *testing.T) {
  rows := &rowsCloseChecker{&fakeRows{}, &simpleCloseChecker{closeError: closeError, noDupClose: true}}
  stream := ReadRows(rows)
  if _, err := toIntAndStringArray(stream); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
  closeVerifyResult(t, stream, closeError)
}

func TestReadRowsManualClose(t *testing.T) {
  rows := &rowsCloseChecker{&fakeRows{}, &simpleCloseChecker{noDupClose: true}}
  verifyDupClose(t, ReadRows(rows))
  verifyCloseCalled(t, rows)
}

func TestReadRowsManualCloseNoImplCloser(t *testing.T) {
  verifyDupClose(t, ReadRows(&fakeRows{}))
}
  
func TestReadLines(t *testing.T) {
  reader := &readerCloseChecker{
      strings.NewReader("Now is\nthe time\nfor all good men.\n"),
      &simpleCloseChecker{noDupClose: true}}
  stream := ReadLines(reader)
  results, err := toStringArray(stream)
  if output := strings.Join(results,","); output != "Now is,the time,for all good men."  {
    t.Errorf("Expected 'Now is,the time,for all good men' got '%v'", output)
  }
  verifyCloseCalled(t, reader)
  verifyDone(t, stream, new(string), err)
}

func TestReadLinesNoImplCloser(t *testing.T) {
  reader := strings.NewReader("Now is\nthe time\nfor all good men.\n")
  stream := ReadLines(reader)
  results, err := toStringArray(stream)
  if output := strings.Join(results,","); output != "Now is,the time,for all good men."  {
    t.Errorf("Expected 'Now is,the time,for all good men' got '%v'", output)
  }
  verifyDone(t, stream, new(string), err)
}

func TestReadLinesLongLine(t *testing.T) {
  str := strings.Repeat("a", 4001) + strings.Repeat("b", 4001) + strings.Repeat("c", 4001) + "\n" + "foo"
  reader := &readerCloseChecker{
      strings.NewReader(str),
      &simpleCloseChecker{noDupClose: true}}
  stream := ReadLines(reader)
  results, err := toStringArray(stream)
  if results[0] != str[0:12003] {
    t.Error("Long line failed.")
  }
  if results[1] != "foo" {
    t.Error("Short line failed")
  }
  if len(results) != 2 {
    t.Error("Results wrong length")
  }
  verifyCloseCalled(t, reader)
  verifyDone(t, stream, new(string), err)
}

func TestReadLinesLongLine2(t *testing.T) {
  str := strings.Repeat("a", 4001) + strings.Repeat("b", 4001) + strings.Repeat("c", 4001)
  reader := &readerCloseChecker{
      strings.NewReader(str),
      &simpleCloseChecker{noDupClose: true}}
  stream := ReadLines(reader)
  results, err := toStringArray(stream)
  if results[0] != str {
    t.Error("Long line failed.")
  }
  if len(results) != 1 {
    t.Error("Results wrong length")
  }
  verifyCloseCalled(t, reader)
  verifyDone(t, stream, new(string), err)
}

func TestReadLinesNextPropagateClose(t *testing.T) {
  reader := &readerCloseChecker{strings.NewReader(""), &simpleCloseChecker{closeError: closeError, noDupClose: true}}
  stream := ReadLines(reader)
  if _, err := toStringArray(stream); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
  closeVerifyResult(t, stream, closeError)
}

func TestReadLinesManualClose(t *testing.T) {
  reader := &readerCloseChecker{strings.NewReader(""), &simpleCloseChecker{noDupClose: true}}
  verifyDupClose(t, ReadLines(reader))
  verifyCloseCalled(t, reader)
}

func TestReadLinesManualCloseNoImplCloser(t *testing.T) {
  reader := strings.NewReader("")
  verifyDupClose(t, ReadLines(reader))
}

func TestNilStream(t *testing.T) {
  stream := NilStream()
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]" {
    t.Errorf("Expected [] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestConcat(t *testing.T) {
  stream := Concat(xrange(5, 8), NilStream(), xrange(9, 11))
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[5 6 7 9 10]"  {
    t.Errorf("Expected [5 6 7 9 10] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestConcat2(t *testing.T) {
  stream := Concat(NilStream(), xrange(7, 9), NilStream())
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[7 8]"  {
    t.Errorf("Expected [7 8] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestConcatEmpty(t *testing.T) {
  stream := Concat()
  if stream != NilStream() {
    t.Error("Did not get the nil stream.")
  }
}

func TestConcatSingle(t *testing.T) {
  s := xrange(7, 9)
  if s != Concat(s) {
    t.Error("Concat should return its single argument.")
  }
}

func TestConcatAllEmptyStreams(t *testing.T) {
  stream := Concat(NilStream(), NilStream())
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestConcatCloseEmpty(t *testing.T) {
  stream := Concat()
  closeVerifyResult(t, stream, nil)
}

func TestConcatCloseNormal(t *testing.T) {
  x := &streamCloseChecker{NilStream(), &simpleCloseChecker{}}
  y := &streamCloseChecker{NilStream(), &simpleCloseChecker{}}
  stream := Concat(x, y)
  closeVerifyResult(t, stream, nil)
  verifyCloseCalled(t, x, y)
}

func TestConcatCloseError1(t *testing.T) {
  x := &streamCloseChecker{NilStream(), &simpleCloseChecker{closeError: closeError}}
  y := &streamCloseChecker{NilStream(), &simpleCloseChecker{}}
  stream := Concat(x, y)
  closeVerifyResult(t, stream, closeError)
  verifyCloseCalled(t, x, y)
}

func TestConcatCloseError2(t *testing.T) {
  x := &streamCloseChecker{NilStream(), &simpleCloseChecker{}}
  y := &streamCloseChecker{NilStream(), &simpleCloseChecker{closeError: closeError}}
  stream := Concat(x, y)
  closeVerifyResult(t, stream, closeError)
  verifyCloseCalled(t, x, y)
}

func TestDeferred(t *testing.T) {
  stream := Deferred(func() Stream { return xrange(10, 12) })
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[10 11]"  {
    t.Errorf("Expected [10 11] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestDeferredCloseNotStarted(t *testing.T) {
  s := &streamCloseChecker{NilStream(), &simpleCloseChecker{closeError: closeError}}
  stream := Deferred(func() Stream { return s })
  closeVerifyResult(t, stream, nil)
}

func TestDeferredCloseError(t *testing.T) {
  s := &streamCloseChecker{xrange(2, 5), &simpleCloseChecker{closeError: closeError}}
  stream := Deferred(func() Stream { return s })
  stream.Next(new(int))
  closeVerifyResult(t, stream, closeError)
  verifyCloseCalled(t, s)
}

func TestDeferredClose(t *testing.T) {
  s := &streamCloseChecker{xrange(2, 5), &simpleCloseChecker{}}
  stream := Deferred(func() Stream { return s })
  stream.Next(new(int))
  closeVerifyResult(t, stream, nil)
  verifyCloseCalled(t, s)
}

func TestCycle(t *testing.T) {
  stream := Slice(
      Cycle(func() Stream { return xrange(10, 12) }), 0, 5)
  results, _ := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[10 11 10 11 10]"  {
    t.Errorf("Expected [10 11 10 11 10] got %v", output)
  }
}

func TestNewStreamFromValues(t *testing.T) {
  stream := NewStreamFromValues([]int{4, 7, 9}, nil)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[4 7 9]"  {
    t.Errorf("Expected [4 7 9] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestNewStreamFromValuesWithCopier(t *testing.T) {
  stream := NewStreamFromValues([]int{4, 7, 9}, squareIntCopier)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[16 49 81]"  {
    t.Errorf("Expected [16 49 81] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestNewStreamFromValuesEmpty(t *testing.T) {
  stream := NewStreamFromValues([]int{}, nil)
  if stream != NilStream() {
    t.Error("Did not get the nil stream.")
  }
}
  
func TestNewStreamFromPtrs(t *testing.T) {
  stream := NewStreamFromPtrs([]*int{ptrInt(4), ptrInt(7), ptrInt(9)}, nil)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[4 7 9]"  {
    t.Errorf("Expected [4 7 9] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestNewStreamFromPtrsWithCopier(t *testing.T) {
  stream := NewStreamFromPtrs([]*int{ptrInt(4), ptrInt(7), ptrInt(9)}, squareIntCopier)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[16 49 81]"  {
    t.Errorf("Expected [16 49 81] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestNewStreamFromPtrsEmpty(t *testing.T) {
  stream := NewStreamFromPtrs([]*int{}, nil)
  if stream != NilStream() {
    t.Error("Did not get the nil stream.")
  }
}
  
func TestFlatten(t *testing.T) {
  if result := getNthDigit(15); result != 2 {
    t.Errorf("Expected 2 got %v", result)
  }
  if result := getNthDigit(300); result != 6 {
    t.Errorf("Expected 6 got %v", result)
  }
  if result := getNthDigit(188); result != 9 {
    t.Errorf("Expected 9 got %v", result)
  }
}

func TestFlattenWithEmptyStreams(t *testing.T) {
  first := NewStreamFromValues([]int{}, nil)
  second := NewStreamFromValues([]int{2}, nil)
  third := NewStreamFromValues([]int{}, nil)
  s := NewStreamFromValues([]Stream{first, second, third}, nil)
  stream := Flatten(s)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[2]" {
    t.Errorf("Expected [2] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestFlattenCloseNormal(t *testing.T) {
  first := NewStreamFromValues([]int{1, 2}, nil)
  second := &streamCloseChecker{
      NewStreamFromValues([]int{3, 4}, nil), &simpleCloseChecker{}}
  s := &streamCloseChecker{
      NewStreamFromValues([]Stream{first, second}, nil),
      &simpleCloseChecker{}}
  stream := Flatten(s)

  // Implicitly closes stream after reading 3rd element
  _, err := toIntArray(Slice(stream, 0, 3))
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
  verifyCloseCalled(t, s, second)
}

func TestFlattenCloseError1(t *testing.T) {
  first := NewStreamFromValues([]int{1, 2}, nil)
  second := &streamCloseChecker{
      NewStreamFromValues([]int{3, 4}, nil),
      &simpleCloseChecker{closeError: closeError}}
  s := &streamCloseChecker{
      NewStreamFromValues([]Stream{first, second}, nil),
      &simpleCloseChecker{}}
  stream := Flatten(s)

  // Implicitly closes stream after reading 3rd element
  _, err := toIntArray(Slice(stream, 0, 3))
  if err != closeError {
    t.Errorf("Expected closeError got %v", err)
  }
  verifyCloseCalled(t, s, second)
}

func TestFlattenCloseError2(t *testing.T) {
  first := NewStreamFromValues([]int{1, 2}, nil)
  second := &streamCloseChecker{
      NewStreamFromValues([]int{3, 4}, nil),
      &simpleCloseChecker{}}
  s := &streamCloseChecker{
      NewStreamFromValues([]Stream{first, second}, nil),
      &simpleCloseChecker{closeError: closeError}}
  stream := Flatten(s)

  // Implicitly closes stream after reading 3rd element
  _, err := toIntArray(Slice(stream, 0, 3))
  if err != closeError {
    t.Errorf("Expected closeError got %v", err)
  }
  verifyCloseCalled(t, s, second)
}

func TestTakeWhileNone(t *testing.T) {
  s := &streamCloseChecker{xrange(0, 5), &simpleCloseChecker{}}
  stream := TakeWhile(Any(), s)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]" {
    t.Errorf("Expected [] got %v", output)
  }
  verifyCloseCalled(t, s)
  verifyDone(t, stream, new(int), err)
}

func TestTakeWhileAll(t *testing.T) {
  s := xrange(0, 5)
  stream := TakeWhile(All(), s)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[0 1 2 3 4]" {
    t.Errorf("Expected [0 1 2 3 4] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestTakeWhileError(t *testing.T) {
  s := xrange(0, 1)
  stream := TakeWhile(errFilterer, s)
  if output := stream.Next(new(int)); output != filterError {
    t.Errorf("Expected filterError, got %v", output)
  }
  if output := stream.Next(new(int)); output != Done {
    t.Errorf("Expected Done, got %v", output)
  }
}

func TestTakeWhileSome(t *testing.T) {
  s := &streamCloseChecker{xrange(0, 5), &simpleCloseChecker{}}
  stream := TakeWhile(notEqual(2), s)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[0 1]" {
    t.Errorf("Expected [0 1] got %v", output)
  }
  verifyCloseCalled(t, s)
  verifyDone(t, stream, new(int), err)
}

func TestTakeWhilePropagateClose(t *testing.T) {
  s := &streamCloseChecker{xrange(0, 5), &simpleCloseChecker{closeError: closeError}}
  stream := TakeWhile(notEqual(2), s)
  if _, err := toIntArray(stream); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
}

func TestDropWhileNone(t *testing.T) {
  s := xrange(0, 5)
  stream := DropWhile(Any(), s)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[0 1 2 3 4]" {
    t.Errorf("Expected [0 1 2 3 4] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestDropWhileAll(t *testing.T) {
  s := xrange(0, 5)
  stream := DropWhile(All(), s)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]" {
    t.Errorf("Expected [] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestDropWhileError(t *testing.T) {
  s := xrange(0, 1)
  stream := DropWhile(errFilterer, s)
  if output := stream.Next(new(int)); output != filterError {
    t.Errorf("Expected filterError, got %v", output)
  }
  if output := stream.Next(new(int)); output != Done {
    t.Errorf("Expected Done, got %v", output)
  }
}

func TestDropWhileSome(t *testing.T) {
  s := xrange(0, 5)
  stream := DropWhile(notEqual(2), s)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[2 3 4]" {
    t.Errorf("Expected [2 3 4] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
}

func TestAny(t *testing.T) {
  a := Any(equal(1), equal(2))
  b := Any()
  c := Any(equal(3))
  d := equal(4)
  e := Any(a, b, c, d)
  for i := 1; i <= 4; i++ {
    if output := e.Filter(ptrInt(i)); output != nil {
      t.Errorf("Expected nil, got %v", output)
    }
  }
  if output := e.Filter(ptrInt(0)); output != Skipped {
    t.Errorf("Expected Skipped, got %v", output)
  }
  if x := len(e.(orFilterer)); x != 4 {
    t.Errorf("Expected 4, got %v", x)
  }
}

func TestAll(t *testing.T) {
  a := All(notEqual(1), notEqual(2))
  b := All()
  c := All(notEqual(3))
  d := notEqual(4)
  e := All(a, b, c, d)
  for i := 1; i <= 4; i++ {
    if output := e.Filter(ptrInt(i)); output != Skipped {
      t.Errorf("Expected Skipped, got %v", output)
    }
  }
  if output := e.Filter(ptrInt(0)); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
  if x := len(e.(andFilterer)); x != 4 {
    t.Errorf("Expected 4, got %v", x)
  }
}

func TestAllWithFilter(t *testing.T) {
  verifyAllWithFilterer(t, skipFilterer, errFilterer, Skipped, Done)
  verifyAllWithFilterer(t, errFilterer, skipFilterer, filterError, filterError)
  verifyAllWithFilterer(t, includeFilterer, errFilterer, filterError, filterError)
  verifyAllWithFilterer(t, errFilterer, includeFilterer, filterError, filterError)
  verifyAllWithFilterer(t, skipFilterer, includeFilterer, Skipped, Done)
  verifyAllWithFilterer(t, includeFilterer, skipFilterer, Skipped, Done)
  verifyAllWithFilterer(t, includeFilterer, includeFilterer, nil, nil)
}

func TestAllWithMapper(t *testing.T) {
  verifyAllWithMapper(t, skipMapper, errMapper, Skipped, Done)
  verifyAllWithMapper(t, errMapper, skipMapper, mapError, mapError)
  verifyAllWithMapper(t, idMapper, errMapper, mapError, mapError)
  verifyAllWithMapper(t, errMapper, idMapper, mapError, mapError)
  verifyAllWithMapper(t, skipMapper, idMapper, Skipped, Done)
  verifyAllWithMapper(t, idMapper, skipMapper, Skipped, Done)
  verifyAllWithMapper(t, idMapper, idMapper, nil, nil)
}

func TestAllAnyComposition(t *testing.T) {
  a := All(
    Any(equal(1), equal(2), equal(3)),
    Any(equal(4), equal(5)))
  if x := len(a.(andFilterer)); x != 2 {
    t.Errorf("Expected 2, got %v", x)
  }
}

func TestAnyAllComposition(t *testing.T) {
  a := Any(
    All(equal(1), equal(2), equal(3)),
    All(equal(4), equal(5)))
  if x := len(a.(orFilterer)); x != 2 {
    t.Errorf("Expected 2, got %v", x)
  }
}

func TestEmptyAny(t *testing.T) {
  a := Any()
  if output := a.Filter(ptrInt(0)); output != Skipped {
    t.Errorf("Expected Skipped, got %v", output)
  }
}

func TestEmptyAll(t *testing.T) {
  a := All()
  if output := a.Filter(ptrInt(0)); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
}

func TestSingleAny(t *testing.T) {
  if trueF != Any(trueF) {
    t.Error("Any should return its single argument.")
  }
}

func TestSingleAll(t *testing.T) {
  if trueF != All(trueF) {
    t.Error("All should return its single argument.")
  }
}

func TestCompose(t *testing.T) {
  f := squareIntInt32
  g := doubleInt32Int64
  h := int64Plus1
  c := Compose(g, f, func() interface{} { return new(int32)})
  c = Compose(h, c, func() interface{} { return new(int64)})
  if x := len(c.pieces()); x != 3 {
    t.Errorf("Expected 3, got %v", x)
  }
  var result int64
  if output := c.Map(ptrInt(5), &result); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
  if result != 51 {
    t.Errorf("Expected 51, got %v", result)
  }
  var fastResult int64
  if output := c.Fast().Map(ptrInt(5), &fastResult); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
  if fastResult != 51 {
    t.Errorf("Expected 51, got %v", fastResult)
  }
}  

func TestCompose2(t *testing.T) {
  a := addMapper(1)
  b := addMapper(2)
  c := addMapper(3)
  d := addMapper(4)
  e := addMapper(5)
  f := func() interface{} { return new(int) }
  c1 := Compose(a, b, f)
  c2 := Compose(c, d, f)
  c2 = Compose(e, c2, f)
  c3 := Compose(c1, c2, f)
  var result int
  if output := c3.Map(ptrInt(0), &result); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
  if result != 15 {
    t.Errorf("Expected 15, got %v", result)
  }
  var fastResult int
  if output := c3.Fast().Map(ptrInt(0), &fastResult); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
  if fastResult != 15 {
    t.Errorf("Expected 15, got %v", fastResult)
  }
}  

func TestCompositeMapperZero(t *testing.T) {
  c := CompositeMapper{}
  var x, y int
  if output := c.Map(&x, &y); output != Skipped {
    t.Errorf("Expected Skipped, got %v", output)
  }
  if output := c.Fast().Map(&x, &y); output != Skipped {
    t.Errorf("Expected Skipped, got %v", output)
  }
}

func TestComposeWithZero(t *testing.T) {
  f := squareIntInt32
  c := Compose(f, CompositeMapper{}, func() interface{} { return new(int)})
  if x := len(c.pieces()); x != 2 {
    t.Errorf("Expected 2, got %v", x)
  }
  var result int32
  if output := c.Map(ptrInt(5), &result); output != Skipped {
    t.Errorf("Expected Skipped, got %v", output)
  }
  c = Compose(CompositeMapper{}, f, func() interface{} { return new(int32)})
  if x := len(c.pieces()); x != 2 {
    t.Errorf("Expected 2, got %v", x)
  }
  if output := c.Map(ptrInt(5), &result); output != Skipped {
    t.Errorf("Expected Skipped, got %v", output)
  }
}

func TestFastCompose(t *testing.T) {
  f := squareIntInt32
  g := doubleInt32Int64
  h := int64Plus1
  c := FastCompose(g, f, new(int32))
  c = FastCompose(h, c, new(int64))
  if x := len(c.(fastCompositeMapper).pieces); x != 3 {
    t.Errorf("Expected 3, got %v", x)
  }
  var result int64
  if output := c.Map(ptrInt(5), &result); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
  if result != 51 {
    t.Errorf("Expected 51, got %v", result)
  }
}

func TestFastCompose2(t *testing.T) {
  a := addMapper(1)
  b := addMapper(2)
  c := addMapper(3)
  d := addMapper(4)
  e := addMapper(5)
  c1 := FastCompose(a, b, new(int))
  c2 := FastCompose(c, d, new(int))
  c2 = FastCompose(e, c2, new(int))
  c3 := FastCompose(c1, c2, new(int))
  var result int
  if output := c3.Map(ptrInt(0), &result); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
  if result != 15 {
    t.Errorf("Expected 15, got %v", result)
  }
  if output := len(c3.(fastCompositeMapper).pieces); output != 5 {
    t.Errorf("Expected 5, got %v", output)
  }
}  

func TestFastComposeWithZero(t *testing.T) {
  f := squareIntInt32
  c := FastCompose(f, CompositeMapper{}, new(int))
  if x := len(c.(fastCompositeMapper).pieces); x != 2 {
    t.Errorf("Expected 2, got %v", x)
  }
  var result int32
  if output := c.Map(ptrInt(5), &result); output != Skipped {
    t.Errorf("Expected Skipped, got %v", output)
  }
  c = FastCompose(CompositeMapper{}, f, new(int32))
  if x := len(c.(fastCompositeMapper).pieces); x != 2 {
    t.Errorf("Expected 2, got %v", x)
  }
  if output := c.Map(ptrInt(5), &result); output != Skipped {
    t.Errorf("Expected Skipped, got %v", output)
  }
}

func TestComposeFastCompose(t *testing.T) {
  f := squareIntInt32
  g := doubleInt32Int64
  h := int64Plus1
  var c Mapper = Compose(g, f, func() interface{} { return new(int32) })
  c = FastCompose(h, c, new(int64))
  if x := len(c.(fastCompositeMapper).pieces); x != 3 {
    t.Errorf("Expected 3, got %v", x)
  }
  var result int64
  if output := c.Map(ptrInt(5), &result); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
  if result != 51 {
    t.Errorf("Expected 51, got %v", result)
  }
}

func TestFastComposeCompose(t *testing.T) {
  f := squareIntInt32
  g := doubleInt32Int64
  h := int64Plus1
  fc := FastCompose(g, f, new(int32))
  c := Compose(h, fc, func() interface{} { return new(int64)})
  if x := len(c.pieces()); x != 3 {
    t.Errorf("Expected 3, got %v", x)
  }
  var result int64
  if output := c.Map(ptrInt(5), &result); output != nil {
    t.Errorf("Expected nil, got %v", output)
  }
  if result != 51 {
    t.Errorf("Expected 51, got %v", result)
  }
}

func TestNoCloseStream(t *testing.T) {
  s := &streamCloseChecker{Count(), &simpleCloseChecker{}}
  stream := Slice(NoCloseStream(s), 0, 3)
  toIntArray(stream)
  if s.closeCalled() {
    t.Error("Did not expect close to be called on underlying stream.")
  }
}

func TestNoCloseRows(t *testing.T) {
  rows := &rowsCloseChecker{
      &fakeRows{ids: []int {}, names: []string{}},
      &simpleCloseChecker{}}
  stream := ReadRows(NoCloseRows(rows))
  toIntAndStringArray(stream)
  if rows.closeCalled() {
    t.Error("Expected rows not to be closed.")
  }
}

func TestNoCloseReader(t *testing.T) {
  reader := &readerCloseChecker{
      strings.NewReader(""),
      &simpleCloseChecker{}}
  stream := ReadLines(NoCloseReader(reader))
  toStringArray(stream)
  if reader.closeCalled() {
    t.Error("Did not expect close to be called on reader.")
  }
}

func verifyDupClose(t *testing.T, c io.Closer) {
  closeVerifyResult(t, c, nil)
  closeVerifyResult(t, c, nil)
}

func closeVerifyResult(t *testing.T, c io.Closer, expected error) {
  if err := c.Close(); err != expected {
    t.Errorf("Expected %v on close, got %v", expected, err)
  }
}

func verifyCloseCalled(t *testing.T, closed ...closeChecker) {
  for i := range closed {
    if !closed[i].closeCalled() {
      t.Error("Expected Close called on all underlying streams.")
      break
    }
  }
}

func verifyDone(t *testing.T, s Stream, ptr interface{}, err error) {
  if err != Done {
    t.Errorf("Expected Done, got %v", err)
  }
  if output := s.Next(ptr); output != Done {
    t.Errorf("Expected Next to keep returning Done, got %v", output)
  }
  if output := s.Close(); output != nil {
    t.Errorf("Expected nil when closing Done stream, got %v", output)
  }
}

func verifyAllWithFilterer(t *testing.T, first Filterer, second Filterer, allError error, streamError error) {
  f := All(first, second)
  stream := Filter(second, Filter(first, NewStreamFromValues([]int {0}, nil)))
  if output := f.Filter(ptrInt(0)); output != allError {
    t.Errorf("Expected %v got %v", allError, output)
  }
  if output := stream.Next(new(int)); output != streamError {
    t.Errorf("Expected %v got %v", streamError, output)
  }
  stream.Close()
}

func verifyAllWithMapper(t *testing.T, first Mapper, second Mapper, allError error, streamError error) {
  m := FastCompose(second, first, new(int))
  stream := Map(second, Map(first, NewStreamFromValues([]int {0}, nil), new(int)), new(int))
  if output := m.Map(ptrInt(0), new(int)); output != allError {
    t.Errorf("Expected %v got %v", allError, output)
  }
  if output := stream.Next(new(int)); output != streamError {
    t.Errorf("Expected %v got %v", streamError, output)
  }
  stream.Close()
}

type intAndString struct {
  id int
  name string
}

func (t *intAndString) Ptrs() []interface{} {
  return []interface{}{&t.id, &t.name}
}

type fakeRows struct {
  ids []int
  names []string
  idx int
}

func (f *fakeRows) Next() bool {
  if f.idx == len(f.ids) || f.idx == len(f.names) {
    return false
  }
  f.idx++
  return true
}

func (f *fakeRows) Scan(args ...interface{}) error {
  p, q := args[0].(*int), args[1].(*string)
  *p = f.ids[f.idx - 1]
  *q = f.names[f.idx - 1]
  return nil
}

type fakeRowsError struct {}

func (f fakeRowsError) Next() bool {
  return true
}

func (f fakeRowsError) Scan(args ...interface{}) error {
  return scanError
}

type closeChecker interface {
  io.Closer
  closeCalled() bool
}

type streamCloseChecker struct {
  Stream
  closeChecker
}

func (s *streamCloseChecker) Close() error {
  checkerResult := s.closeChecker.Close()
  streamResult := s.Stream.Close()
  if checkerResult == nil {
    return streamResult
  }
  return checkerResult
}

type rowsCloseChecker struct {
  Rows
  closeChecker
}

type readerCloseChecker struct {
  io.Reader
  closeChecker
}

type simpleCloseChecker struct {
  closeError error
  closeCount int
  noDupClose bool
}

func (sc *simpleCloseChecker) Close() error {
  sc.closeCount++
  if sc.noDupClose && sc.closeCount > 1 {
    panic("duplicate close not allowed.")
  }
  return sc.closeError
}

func (sc *simpleCloseChecker) closeCalled() bool {
  return sc.closeCount > 0
}

type errorFilterer struct {
  e error
}

func (f errorFilterer) Filter(ptr interface{}) error {
  return f.e
}

type errorMapper struct {
  e error
}

func (m errorMapper) Map(srcPtr, destPtr interface{}) error {
  return m.e
}

// getNthDigit returns the nth digit in the sequence:
// 12345678910111213141516... getNthDigit(1) == 1.
func getNthDigit(x int) int {
  s := Slice(digitStream(), x - 1, -1)
  var result int
  s.Next(&result)
  s.Close()
  return result
}

// digitStream returns a Stream of int = 1,2,3,4,5,6,7,8,9,1,0,1,1,...
func digitStream() Stream {
  return Flatten(Map(&intToDigitsMapper{}, Count(), new(int)))
}

// intToDigitsMapper converts an int into a Stream of int that emits its digits,
// most significant first.
type intToDigitsMapper struct {
  digits []int
}

// Map maps 123 -> {1, 2, 3}. Resulting Stream is valid until the next call
// to Map.
func (m *intToDigitsMapper) Map(srcPtr, destPtr interface{}) error {
  x := *(srcPtr.(*int))
  result := destPtr.(*Stream)
  m.digits = m.digits[:0]
  for x > 0 {
    m.digits = append(m.digits, x % 10)
    x /= 10
  }
  for i := 0; i < len(m.digits) - i - 1; i++ {
    temp := m.digits[i]
    m.digits[i] = m.digits[len(m.digits) - i - 1]
    m.digits[len(m.digits) - i - 1] = temp
  }
  *result = NewStreamFromValues(m.digits, nil)
  return nil
}

type addMapper int

func (m addMapper) Map(srcPtr, destPtr interface{}) error {
  x := srcPtr.(*int)
  y := destPtr.(*int)
  *y = *x + int(m)
  return nil
}

func squareIntCopier(src interface{}, dest interface{}) {
  d := dest.(*int)
  s := src.(*int)
  *d = (*s) * (*s)
}

func xrange(start, end int) Stream {
  return Slice(Count(), start, end)
}

func lessThan(x int) Filterer {
  return NewFilterer(func(ptr interface{}) error {
    p := ptr.(*int)
    if *p < x {
      return nil
    }
    return Skipped
  })
}

func greaterThan(x int) Filterer {
  return NewFilterer(func(ptr interface{}) error {
    p := ptr.(*int)
    if *p > x {
      return nil
    }
    return Skipped
  })
}

func notEqual(x int) Filterer {
  return NewFilterer(func(ptr interface{}) error {
    p := ptr.(*int)
    if *p != x {
      return nil
    }
    return Skipped
  })
}

func equal(x int) Filterer {
  return NewFilterer(func(ptr interface{}) error {
    p := ptr.(*int)
    if *p == x {
      return nil
    }
    return Skipped
  })
}

func ptrInt(x int) *int {
  return &x
}

func toStringArray(s Stream) ([]string, error) {
  var result []string
  var x string
  err := s.Next(&x)
  for ;err == nil; err = s.Next(&x) {
    result = append(result, x)
  }
  return result, err
}

func toIntArray(s Stream) ([]int, error) {
  var result []int
  var x int
  err := s.Next(&x)
  for ;err == nil; err = s.Next(&x) {
    result = append(result, x)
  }
  return result, err
}

func toInt32Array(s Stream) ([]int32, error) {
  var result []int32
  var x int32
  err := s.Next(&x)
  for ;err == nil; err = s.Next(&x) {
    result = append(result, x)
  }
  return result, err
}

func toInt64Array(s Stream) ([]int64, error) {
  var result []int64
  var x int64
  err := s.Next(&x)
  for ;err == nil; err = s.Next(&x) {
    result = append(result, x)
  }
  return result, err
}

func toIntAndStringArray(s Stream) ([]intAndString, error) {
  var result []intAndString
  var x intAndString
  err := s.Next(&x)
  for ;err == nil; err = s.Next(&x) {
    result = append(result, x)
  }
  return result, err
}
