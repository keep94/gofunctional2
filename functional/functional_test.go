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
  alreadyClosedError = errors.New("already closed.")
  int64Plus1 = NewMapper(
      func (srcPtr interface{}, destPtr interface{}) bool {
        p := srcPtr.(*int64)
        q := destPtr.(*int64)
        *q = (*p) + 1
        return true
      })
  doubleInt32Int64 = NewMapper(
      func (srcPtr interface{}, destPtr interface{}) bool {
        p := srcPtr.(*int32)
        q := destPtr.(*int64)
        *q = 2 * int64(*p)
        return true
      })
  squareIntInt32 = NewMapper(
      func (srcPtr interface{}, destPtr interface{}) bool {
        p := srcPtr.(*int)
        q := destPtr.(*int32)
        *q = int32(*p) * int32(*p)
        return true
  })
)

func TestFilterAndMap(t *testing.T) {
  s := xrange(5, 15)
  f := NewFilterer(func(ptr interface{}) bool {
    p := ptr.(*int)
    return *p % 2 == 0
  })
  m := NewMapper(func(srcPtr interface{}, destPtr interface{}) bool {
    s := srcPtr.(*int)
    d := destPtr.(*int32)
    *d = int32((*s) * (*s))
    return true
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
  m := NewMapper(func(srcPtr interface{}, destPtr interface{}) bool {
    s := srcPtr.(*int)
    d := destPtr.(*int32)
    if *s % 2 != 0 {
      return false
    }
    *d = int32((*s) * (*s))
    return true
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
  s := streamCloseChecker{xrange(5, 13), &simpleCloseChecker{}}
  stream := Slice(s, 2, 4)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[7 8]"  {
    t.Errorf("Expected [7 8] got %v", output)
  }
  verifyClosed(t, s)
  verifyDone(t, stream, new(int), err)
}

func TestSliceWithEnd2(t *testing.T) {
  s := streamCloseChecker{xrange(5, 13), &simpleCloseChecker{}}
  stream := Slice(s, 0, 2)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[5 6]"  {
    t.Errorf("Expected [5 6] got %v", output)
  }
  verifyClosed(t, s)
  verifyDone(t, stream, new(int), err)
}

func TestZeroSlice(t *testing.T) {
  s := streamCloseChecker{xrange(5, 13), &simpleCloseChecker{}}
  stream := Slice(s, 2, 2)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  verifyClosed(t, s)
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
  s := streamCloseChecker{xrange(5, 13), &simpleCloseChecker{}}
  stream := Slice(s, 4, 3)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
  verifyClosed(t, s)
}

func TestSliceNextPropagateClose(t *testing.T) {
  s := streamCloseChecker{Count(), &simpleCloseChecker{closeError: closeError}}
  stream := Slice(s, 7, 10)
  if _ ,err := toIntArray(stream); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
  if err := stream.Close(); err != closeError {
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
  rows := &fakeRows{ids: []int {3, 4}, names: []string{"foo", "bar"}}
  stream := ReadRows(rows)
  results, err := toIntAndStringArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[{3 foo} {4 bar}]"  {
    t.Errorf("Expected [{3 foo} {4 bar}] got %v", output)
  }
  verifyDone(t, stream, new(intAndString), err)
} 

func TestReadRowsEmpty(t *testing.T) {
  rows := &fakeRows{}
  stream := ReadRows(rows)
  results, err := toIntAndStringArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
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
  rows := rowsCloseChecker{&fakeRows{}, &simpleCloseChecker{closeError: closeError}}
  stream := ReadRows(rows)
  if _, err := toIntAndStringArray(stream); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
  if err := stream.Close(); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
}

func TestReadRowsManualClose(t *testing.T) {
  rows := rowsCloseChecker{&fakeRows{}, &noDupCloseChecker{}}
  verifyDupClose(t, ReadRows(rows))
}
  
func TestReadLines(t *testing.T) {
  r := strings.NewReader("Now is\nthe time\nfor all good men.\n")
  stream := ReadLines(r)
  results, err := toStringArray(stream)
  if output := strings.Join(results,","); output != "Now is,the time,for all good men."  {
    t.Errorf("Expected 'Now is,the time,for all good men' got '%v'", output)
  }
  verifyDone(t, stream, new(string), err)
}

func TestReadLinesLongLine(t *testing.T) {
  str := strings.Repeat("a", 4001) + strings.Repeat("b", 4001) + strings.Repeat("c", 4001) + "\n" + "foo"
  stream := ReadLines(strings.NewReader(str))
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
  verifyDone(t, stream, new(string), err)
}

func TestReadLinesLongLine2(t *testing.T) {
  str := strings.Repeat("a", 4001) + strings.Repeat("b", 4001) + strings.Repeat("c", 4001)
  stream := ReadLines(strings.NewReader(str))
  results, err := toStringArray(stream)
  if results[0] != str {
    t.Error("Long line failed.")
  }
  if len(results) != 1 {
    t.Error("Results wrong length")
  }
  verifyDone(t, stream, new(string), err)
}

func TestReadLinesNextPropagateClose(t *testing.T) {
  reader := readerCloseChecker{strings.NewReader(""), &simpleCloseChecker{closeError: closeError}}
  stream := ReadLines(reader)
  if _, err := toStringArray(stream); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
  if err := stream.Close(); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
}

func TestReadLinesManualClose(t *testing.T) {
  reader := readerCloseChecker{strings.NewReader(""), &noDupCloseChecker{}}
  verifyDupClose(t, ReadLines(reader))
}

func TestNilStream(t *testing.T) {
  stream := NilStream()
  _, err := toIntArray(stream)
  verifyDone(t, stream, new(int), err)
}
  
func TestAny(t *testing.T) {
  a := Any(equal(1), equal(2))
  b := Any()
  c := Any(equal(3))
  d := equal(4)
  e := Any(a, b, c, d)
  for i := 1; i <= 4; i++ {
    if !e.Filter(ptrInt(i)) {
      t.Error("Call to Any failed")
    }
  }
  if e.Filter(ptrInt(0)) {
    t.Error("Call to Any failed")
  }
  if x := len(e.(orFilterer)); x != 4 {
    t.Errorf("Expected length of or filter to be 4, got %v", x)
  }
}

func TestAll(t *testing.T) {
  a := All(notEqual(1), notEqual(2))
  b := All()
  c := All(notEqual(3))
  d := notEqual(4)
  e := All(a, b, c, d)
  for i := 1; i <= 4; i++ {
    if e.Filter(ptrInt(i)) {
      t.Error("Call to All failed")
    }
  }
  if !e.Filter(ptrInt(0)) {
    t.Error("Call to All failed")
  }
  if x := len(e.(andFilterer)); x != 4 {
    t.Errorf("Expected length of and filter to be 4, got %v", x)
  }
}

func TestAllAnyComposition(t *testing.T) {
  a := All(
    Any(equal(1), equal(2), equal(3)),
    Any(equal(4)))
  if x := len(a.(andFilterer)); x != 2 {
    t.Errorf("Expected length of and filter to be 2, got %v", x)
  }
}

func TestAnyAllComposition(t *testing.T) {
  a := Any(
    All(equal(1), equal(2), equal(3)),
    All(equal(4)))
  if x := len(a.(orFilterer)); x != 2 {
    t.Errorf("Expected length of or filter to be 2, got %v", x)
  }
}

func TestEmptyAny(t *testing.T) {
  a := Any()
  if a.Filter(ptrInt(0)) {
    t.Error("Empty Any failed.")
  }
}
  
func TestEmptyAll(t *testing.T) {
  a := All()
  if !a.Filter(ptrInt(0)) {
    t.Error("Empty All failed.")
  }
}

func TestCompose(t *testing.T) {
  f := squareIntInt32
  g := doubleInt32Int64
  h := int64Plus1
  var i32 int32
  var i64 int64
  c := Compose(g, f, func() interface{} { return new(int32)})
  c = Compose(h, c, func() interface{} { return new(int64)})
  if x := len(c.(*compositeMapper).mappers); x != 3 {
    t.Error("Composition of composite mapper wrong.")
  }
  var result int64
  if !c.Map(ptrInt(5), &result) {
    t.Error("Map returns false instead of true.")
  }
  if result != 51 {
    t.Error("Map returned wrong value.")
  }
  if i32 != 0 || i64 != 0 {
    t.Error("Mapper not thread safe.")
  }
}  

func verifyDupClose(t *testing.T, s Stream) {
  if err := s.Close(); err != nil {
    t.Errorf("Expected nil on close got %v", err)
  }
  if err := s.Close(); err != nil {
    t.Errorf("Expected nil on close got %v", err)
  }
}

func verifyClosed(t *testing.T, closed ...closeChecker) {
  for i := range closed {
    if !closed[i].isClosed() {
      t.Error("Expected all underlying streams closed.")
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
  isClosed() bool
}

type streamCloseChecker struct {
  Stream
  closeChecker
}

func (s streamCloseChecker) Close() error {
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
  closeCalled bool
}

func (sc *simpleCloseChecker) Close() error {
  sc.closeCalled = true
  return sc.closeError
}

func (sc *simpleCloseChecker) isClosed() bool {
  return sc.closeCalled
}

type noDupCloseChecker struct {
  closeCount int
}

func (c *noDupCloseChecker) Close() error {
  c.closeCount++
  if c.closeCount == 1 {
    return nil
  }
  return alreadyClosedError
}

func (c *noDupCloseChecker) isClosed() bool {
  return c.closeCount > 0
}

func xrange(start, end int) Stream {
  return Slice(Count(), start, end)
}

func lessThan(x int) Filterer {
  return NewFilterer(func(ptr interface{}) bool {
    p := ptr.(*int)
    return *p < x
  })
}

func greaterThan(x int) Filterer {
  return NewFilterer(func(ptr interface{}) bool {
    p := ptr.(*int)
    return *p > x
  })
}

func notEqual(x int) Filterer {
  return NewFilterer(func(ptr interface{}) bool {
    p := ptr.(*int)
    return *p != x
  })
}

func equal(x int) Filterer {
  return NewFilterer(func(ptr interface{}) bool {
    p := ptr.(*int)
    return *p == x
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
