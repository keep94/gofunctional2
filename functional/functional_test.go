package functional

import (
    "errors"
    "fmt"
    "testing"
)

var (
  scanError = errors.New("error scanning.")
  closeError = errors.New("error closing.")
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
  results, err := toInt32Array(Map(m, Filter(f, s), new(int)))
  if output := fmt.Sprintf("%v", results); output != "[36 64 100 144 196]"  {
    t.Errorf("Expected [36 64 100 144 196] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
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
  results, err := toInt64Array(Map(doubleInt32Int64, Map(m, s, new(int)), new(int32)))
  if output := fmt.Sprintf("%v", results); output != "[72 128 200 288 392]"  {
    t.Errorf("Expected [64 128 200 288 392] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
}

func TestNoFilterInFilter(t *testing.T) {
  s := Filter(greaterThan(5), Filter(lessThan(8), xrange(0, 10)))
  _, filterInFilter := s.(*filterStream).Stream.(*filterStream)
  if filterInFilter {
    t.Error("Got a filter within a filter.")
  }
}

func TestNestedFilter(t *testing.T) {
  s := Filter(greaterThan(5), Filter(lessThan(8), xrange(0, 10)))
  results, err := toIntArray(s)
  if output := fmt.Sprintf("%v", results); output != "[6 7]"  {
    t.Errorf("Expected [6 7] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
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
  results, err := toInt64Array(Map(doubleInt32Int64, s, new(int32)))
  if output := fmt.Sprintf("%v", results); output != "[18 32 50]"  {
    t.Errorf("Expected [18 32 50] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
}

func TestSliceNoEnd(t *testing.T) {
  s := xrange(5, 13)
  results, err := toIntArray(Slice(s, 5, -1))
  if output := fmt.Sprintf("%v", results); output != "[10 11 12]"  {
    t.Errorf("Expected [10 11 12] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
}

func TestSliceWithEnd(t *testing.T) {
  s := &detectClose{Stream: xrange(5, 13)}
  results, err := toIntArray(Slice(s, 2, 4))
  if output := fmt.Sprintf("%v", results); output != "[7 8]"  {
    t.Errorf("Expected [7 8] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
  verifyClosed(t, s)
}

func TestSliceWithEnd2(t *testing.T) {
  s := &detectClose{Stream: xrange(5, 13)}
  results, err := toIntArray(Slice(s, 0, 2))
  if output := fmt.Sprintf("%v", results); output != "[5 6]"  {
    t.Errorf("Expected [5 6] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
  verifyClosed(t, s)
}

func TestZeroSlice(t *testing.T) {
  s := &detectClose{Stream: xrange(5, 13)}
  results, err := toIntArray(Slice(s, 2, 2))
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
  verifyClosed(t, s)
}

func TestSliceStartTooBig(t *testing.T) {
  s := xrange(5, 13)
  results, err := toIntArray(Slice(s, 20, 30))
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
}

func TestSliceEndTooBig(t *testing.T) {
  s := xrange(5, 13)
  results, err := toIntArray(Slice(s, 7, 10))
  if output := fmt.Sprintf("%v", results); output != "[12]"  {
    t.Errorf("Expected [12] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
}

func TestSliceStartBiggerThanEnd(t *testing.T) {
  s := &detectClose{Stream: xrange(5, 13)}
  results, err := toIntArray(Slice(s, 4, 3))
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
  verifyClosed(t, s)
}

func TestSliceAutoClose(t *testing.T) {
  s := &detectClose{Stream: xrange(0, 20), closeError: closeError}
  verifyCloseOnDone(t, Slice(s, 7, 10), new(int), closeError, s)
  s = &detectClose{Stream: xrange(0, 20)}
  verifyCloseOnDone(t, Slice(s, 7, 10), new(int), Done, s)
}
  
func TestSliceManualClose(t *testing.T) {
  s := &detectClose{Stream: xrange(0, 20), closeError: closeError}
  verifyClose(t, Slice(s, 7, 10), new(int), closeError, s)
  s = &detectClose{Stream: xrange(0, 20)}
  verifyClose(t, Slice(s, 7, 10), new(int), nil, s)
}

func TestCountFrom(t *testing.T) {
  results, err := toIntArray(Slice(CountFrom(5, 2), 1, 3))
  if output := fmt.Sprintf("%v", results); output != "[7 9]"  {
    t.Errorf("Expected [7 9] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
}

func TestReadRows(t *testing.T) {
  rows := &fakeRows{ids: []int {3, 4}, names: []string{"foo", "bar"}}
  results, err := toIntAndStringArray(ReadRows(rows))
  if output := fmt.Sprintf("%v", results); output != "[{3 foo} {4 bar}]"  {
    t.Errorf("Expected [{3 foo} {4 bar}] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
} 

func TestReadRowsAutoClose(t *testing.T) {
  rows := &rowsDetectClose{Rows: &fakeRows{}}
  verifyCloseOnDone(t, ReadRows(rows), new(intAndString), Done, rows)
  rows = &rowsDetectClose{Rows: &fakeRows{}, closeError: closeError}
  verifyCloseOnDone(t, ReadRows(rows), new(intAndString), closeError, rows)
}

func TestReadRowsManualClose(t *testing.T) {
  rows := &rowsDetectClose{Rows: &fakeRows{}}
  verifyClose(t, ReadRows(rows), new(intAndString), nil, rows)
  rows = &rowsDetectClose{Rows: &fakeRows{}, closeError: closeError}
  verifyClose(t, ReadRows(rows), new(intAndString), closeError, rows)
}
  
func TestReadRowsEmpty(t *testing.T) {
  rows := &fakeRows{}
  results, err := toIntAndStringArray(ReadRows(rows))
  if output := fmt.Sprintf("%v", results); output != "[]"  {
    t.Errorf("Expected [] got %v", output)
  }
  if err != Done {
    t.Errorf("Expected Done got %v", err)
  }
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

func verifyCloseOnDone(t *testing.T, s Stream, ptr interface{}, expected error, closed ...closeChecker) {
  if err := consume(s, ptr); err != expected {
    t.Errorf("Expected %v got %v", expected, err)
  }
  verifyClosed(t, closed...)
}

func verifyClose(t *testing.T, s Stream, ptr interface{}, expected error, closed ...closeChecker) {
  if err := s.Close(); err != expected {
    t.Errorf("Expected %v got Tv", expected, err)
  }
  verifyClosed(t, closed...)
  if output := s.Next(ptr); output != Done {
    t.Errorf("Expect Next to return Done after Close, got %v", output)
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

func consume(s Stream, ptr interface{}) error {
  err := s.Next(ptr)
  for err == nil {
    err = s.Next(ptr)
  }
  return err
}

type closeChecker interface {
  isClosed() bool
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

type detectClose struct {
  Stream
  closeError error
  closeCalled bool
}

func (s *detectClose) Close() error {
  s.closeCalled = true
  result := s.Stream.Close()
  if s.closeError == nil {
    return result
  }
  return s.closeError
}

func (s *detectClose) isClosed() bool {
  return s.closeCalled
}

type rowsDetectClose struct {
  Rows
  closeError error
  closeCalled bool
}

func (r *rowsDetectClose) Close() error {
  r.closeCalled = true
  return r.closeError
}

func (r *rowsDetectClose) isClosed() bool {
  return r.closeCalled
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



