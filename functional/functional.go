// Package functional provides functional programming constructs.
package functional

import (
  "errors"
  "io"
)

// Done indicates that the end of a Stream has been reached
var Done = errors.New("functional: End of Stream reached.")

// Stream is a sequence emitted values.
// Each call to Next() emits the next value in the stream.
// A Stream that emits values of type T is a Stream of T.
type Stream interface {
  // Next emits the next value in this Stream of T.
  // If Next returns nil, the next value is stored at ptr.
  // If Next returns Done, then the end of the Stream has been reached,
  // and the value ptr points to is unspecified.
  // If Next returns some other error, then the caller should close the
  // Stream with Close.  ptr must be a *T
  Next(ptr interface{}) error
  // Close indicates that the caller is finished with this Stream. If Caller
  // consumes all the values in this Stream, then it need not call Close. But
  // if Caller chooses not to consume the Stream entirely, it should call
  // Close on it. Caller should also call Close if Next returns an error other
  // than Done.
  io.Closer
}

// Tuple represents a tuple of values that ReadRows emits
type Tuple interface {
  // Ptrs returns a pointer to each field in the tuple.
  Ptrs() []interface{}
}

// Filterer of T filters values in a Stream of T.
type Filterer interface {
  // Filter returns true if value ptr points to should be included or false
  // otherwise. ptr must be a *T.
  Filter(ptr interface{}) bool
}

// Mapper maps a type T value to a type U value in a Stream.
type Mapper interface {
  // Map does the mapping storing the mapped value at destPtr.
  // If Mapper returns false, then no mapped value is stored at destPtr.
  // srcPtr is a *T; destPtr is a *U
  Map(srcPtr interface{}, destPtr interface{}) bool
  // Fast returns a faster version of this Mapper. If a function will use
  // a Mapper more than once, say in a for loop, it should call Fast and use
  // the returned Mapper instead. Returned Mapper should be considered not
  // thread-safe even if this Mapper is. In particular, the returned Mapper
  // may re-use temporary storage rather than creating it anew each time Map
  // is invoked. Most implementations can simply return themselves.
  Fast() Mapper
}

// Creater of T creates a new, pre-initialized, T and returns a pointer to it.
type Creater func() interface {}

// Rows represents rows in a database table. Most database API already have
// a type that implements this interface
type Rows interface {
  // Next advances to the next row. Next returns false if there is no next row.
  // Every call to Scan, even the first one, must be preceded by a call to Next.
  Next() bool
  // Reads the values out of the current row. args are pointer types.
  Scan(args ...interface{}) error
}

// Map applies f, which maps a type T value to a type U value, to a Stream
// of T producing a new Stream of U. If s is
// (x1, x2, x3, ...), Map returns the Stream (f(x1), f(x2), f(x3), ...).
// if f returns false for a T value, then the corresponding U value is left
// out of the returned stream. ptr is a *T providing storage for emitted values
// from s. Clients need not pass f.Fast() to Map because Map calls Fast
// internally. Calling Close on returned Stream closes s.
func Map(f Mapper, s Stream, ptr interface{}) Stream {
  ms, ok := s.(*mapStream)
  if ok {
    return &mapStream{Compose(f, ms.mapper, newCreater(ptr)).Fast(), ms.Stream, ms.ptr}
  }
  return &mapStream{f.Fast(), s, ptr}
}

// Filter filters values from s, returning a new Stream of T.
// Calling Close on returned Stream closes s.
// f is a Filterer of T; s is a Stream of T.
func Filter(f Filterer, s Stream) Stream {
  fs, ok := s.(*filterStream)
  if ok {
    return &filterStream{All(fs.filterer, f), fs.Stream}
  }
  return &filterStream{f, s}
}

// Count returns an infinite Stream of int which emits all values beginning
// at 0. Calling Close on returned Stream is a no-op.
func Count() Stream {
  return &count{0, 1}
}

// CountFrom returns an infinite Stream of int emitting values beginning at
// start and increasing by step. Calling Closeon returned Stream is a no-op.
func CountFrom(start, step int) Stream {
  return &count{start, step}
}

// Slice returns a Stream that will emit elements in s starting at index start
// and continuing to but not including index end. Indexes are 0 based. If end
// is negative, it means go to the end of s. Calling Close on returned Stream
// closes s. When end of returned Stream is reached, it closes s if it has not
// consumed s returning any Close error through Next.
func Slice(s Stream, start int, end int) Stream {
  return &sliceStream{stream: s, start: start, end: end}
}

// ReadRows returns the rows in a database table as a Stream of Tuple. When
// end of returned Stream is reached, it closes r if r implements io.Closer.
// Calling Close on returned stream closes r if r implements io.Closer.
func ReadRows(r Rows) Stream {
  return &rowStream{rows: r}
}

// Any returns a Filterer that returns true if any of the
// fs return true.
func Any(fs ...Filterer) Filterer {
  ors := make([][]Filterer, len(fs))
  for i := range fs {
    ors[i] = orList(fs[i])
  }
  return orFilterer(filterFlatten(ors))
}

// All returns a Filterer that returns true if all of the
// fs return true.
func All(fs ...Filterer) Filterer {
  ands := make([][]Filterer, len(fs))
  for i := range fs {
    ands[i] = andList(fs[i])
  }
  return andFilterer(filterFlatten(ands))
}

// Compose composes two Mappers together into one e.g f(g(x)). If g maps
// type T values to type U values, and f maps type U values to type V
// values, then Compose returns a Mapper mapping T values to V values. c is
// a Creater of U. Each time Map is called on returned Mapper, it invokes c
// to create a U value to receive the intermediate result from g. Calling
// Fast() on returned Mapper creates a new Mapper with this U value already
// pre-initialized.
func Compose(f Mapper, g Mapper, c Creater) Mapper {
  l := mapperLen(f) + mapperLen(g)
  mappers := make([]Mapper, l)
  creaters := make([]Creater, l - 1)
  n := appendMapper(mappers, creaters, g)
  creaters[n - 1] = c
  appendMapper(mappers[n:], creaters[n:], f)
  return &compositeMapper{mappers, creaters, nil}
}

// NewFilterer returns a new Filterer of T. f takes a *T returning true
// if T value pointed to it should be included.
func NewFilterer(f func(ptr interface{}) bool) Filterer {
  return funcFilterer(f)
}

// NewMapper returns a new Mapper mapping T values to U Values. In f,
// srcPtr is a *T and destPtr is a *U pointing to pre-allocated T and U
// values respectively.
func NewMapper(m func(srcPtr interface{}, destPtr interface{}) bool) Mapper {
  return funcMapper(m)
}

type count struct {
  start int
  step int
}

func (c *count) Next(ptr interface{}) error {
  p := ptr.(*int)
  *p = c.start
  c.start += c.step
  return nil
}

func (c *count) Close() error {
  return nil
}

type mapStream struct {
  mapper Mapper
  Stream
  ptr interface{} 
}

func (s *mapStream) Next(ptr interface{}) error {
  err := s.Stream.Next(s.ptr)
  for ; err == nil; err = s.Stream.Next(s.ptr) {
    if s.mapper.Map(s.ptr, ptr) {
      return nil
    }
  }
  return err
}

type filterStream struct {
  filterer Filterer
  Stream
}

func (s *filterStream) Next(ptr interface{}) error {
  err := s.Stream.Next(ptr)
  for ; err == nil; err = s.Stream.Next(ptr) {
    if s.filterer.Filter(ptr) {
      return nil
    }
  }
  return err
}

type sliceStream struct {
  stream Stream
  start int
  end int
  index int
  done bool
}

func (s *sliceStream) Next(ptr interface{}) error {
  if s.done {
    return Done
  }
  if s.end >= 0 && s.start >= s.end {
    return finish(s.Close())
  }
  for s.end < 0 || s.index < s.end {
    err := s.stream.Next(ptr)
    if err == Done {
      s.done = true
      return Done
    }
    if err != nil {
      return err
    }
    s.index++
    if s.index > s.start {
      return nil
    }
  }
  return finish(s.Close())
}

func (s *sliceStream) Close() error {
  s.done = true
  return closeUnder(&s.stream)
}

type rowStream struct {
  rows Rows
  done bool
}

func (r *rowStream) Next(ptr interface{}) error {
  if r.done {
    return Done
  }
  if !r.rows.Next() {
    return finish(r.Close())
  }
  ptrs := ptr.(Tuple).Ptrs()
  return r.rows.Scan(ptrs...)
}

func (r *rowStream) Close() error {
  r.done = true
  if r.rows == nil {
    return nil
  }
  var result error
  c, ok := r.rows.(io.Closer)
  if ok {
    result = c.Close()
  }
  if result == nil {
    r.rows = nil
  }
  return result
}
  
type funcFilterer func(ptr interface{}) bool

func (f funcFilterer) Filter(ptr interface{}) bool {
  return f(ptr)
}

type andFilterer []Filterer

func (f andFilterer) Filter(ptr interface{}) bool {
  for i := range f {
    if !f[i].Filter(ptr) {
      return false
    }
  }
  return true
}

type orFilterer []Filterer

func (f orFilterer) Filter(ptr interface{}) bool {
  for i := range f {
    if f[i].Filter(ptr) {
      return true
    }
  }
  return false
}

type funcMapper func(srcPtr interface{}, destPtr interface{}) bool

func (m funcMapper) Map(srcPtr interface{}, destPtr interface{}) bool {
  return m(srcPtr, destPtr)
}

func (m funcMapper) Fast() Mapper {
  return m
}

type compositeMapper struct {
  mappers []Mapper
  creaters []Creater
  values []interface{}
}

func (m *compositeMapper) Map(srcPtr interface{}, destPtr interface{}) bool {
  if m.values != nil {
    num := len(m.mappers)
    if !m.mappers[0].Map(srcPtr, m.values[0]) {
      return false
    }
    for i := 1; i < num - 1; i++ {
      if !m.mappers[i].Map(m.values[i-1], m.values[i]) {
        return false
      }
    }
    if !m.mappers[num - 1].Map(m.values[num - 2], destPtr) {
      return false
    }
    return true
  }
  return m.Fast().Map(srcPtr, destPtr)
}

func (m *compositeMapper) Fast() Mapper {
  if m.values != nil {
    return m
  }
  return &compositeMapper{m.fastMappers(), m.creaters, m.createValues()}
}

func (m *compositeMapper) createValues() []interface{} {
  result := make([]interface{}, len(m.creaters))
  for i := range m.creaters {
    result[i] = m.creaters[i]()
  }
  return result
}

func (m *compositeMapper) fastMappers() []Mapper {
  result := make([]Mapper, len(m.mappers))
  for i := range m.mappers {
    result[i] = m.mappers[i].Fast()
  }
  return result
}

func orList(f Filterer) []Filterer {
  ors, ok := f.(orFilterer)
  if ok {
    return ors
  }
  return []Filterer{f}
}

func andList(f Filterer) []Filterer {
  ands, ok := f.(andFilterer)
  if ok {
    return ands
  }
  return []Filterer{f}
}

func filterFlatten(fs [][]Filterer) []Filterer {
  var l int
  for i := range fs {
    l += len(fs[i])
  }
  result := make([]Filterer, l)
  n := 0
  for i := range fs {
    n += copy(result[n:], fs[i])
  }
  return result
}

func mapperLen(m Mapper) int {
  cm, ok := m.(*compositeMapper)
  if ok {
    return len(cm.mappers)
  }
  return 1
}

func appendMapper(mappers []Mapper, creaters []Creater, m Mapper) int {
  cm, ok := m.(*compositeMapper)
  if ok {
    copy(creaters, cm.creaters)
    return copy(mappers, cm.mappers)
  }
  mappers[0] = m
  return 1
}

func newCreater(ptr interface{}) Creater {
  return func() interface{} {
    return ptr
  }
}

func finish(e error) error {
  if e == nil {
    return Done
  }
  return e
}

func closeUnder(ptr *Stream) error {
  if *ptr == nil {
    return nil
  }
  result := (*ptr).Close()
  if result == nil {
    *ptr = nil
  }
  return result
}
