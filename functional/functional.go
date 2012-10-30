// Package functional provides functional programming constructs.
package functional

import (
  "bufio"
  "errors"
  "io"
  "reflect"
)

// Done indicates that the end of a Stream has been reached
var (
  Done = errors.New("functional: End of Stream reached.")
  Skipped = errors.New("functional: Value skipped.")
  nilM = nilMapper{}
  nilPieceL = []compositeMapperPiece{{mapper: nilM}}
  nilS = nilStream{}
  trueFilterer = andFilterer(nil)
  falseFilterer = orFilterer(nil)
)

// Stream is a sequence emitted values.
// Each call to Next() emits the next value in the stream.
// A Stream that emits values of type T is a Stream of T.
type Stream interface {
  // Next emits the next value in this Stream of T.
  // If Next returns nil, the next value is stored at ptr.
  // If Next returns Done, then the end of the Stream has been reached,
  // and the value ptr points to is unspecified.
  // If Next returns some other error, then the caller should close the
  // Stream with Close.  ptr must be a *T.
  // Once Next returns Done, it should continue to return Done, and
  // Close should return nil.
  Next(ptr interface{}) error
  // Close indicates that the caller is finished with this Stream. If Caller
  // consumes all the values in this Stream, then it need not call Close. But
  // if Caller chooses not to consume the Stream entirely, it should call
  // Close. Caller should also call Close if Next returns an error other
  // than Done. Once Close returns nil, it should continue to return nil.
  // The result of calling Next after Close is undefined.
  io.Closer
}

// Tuple represents a tuple of values that ReadRows emits
type Tuple interface {
  // Ptrs returns a pointer to each field in the tuple.
  Ptrs() []interface{}
}

// Filterer of T filters values in a Stream of T.
type Filterer interface {
  // Filter returns nil if value ptr points to should be included or Skipped
  // if value should be skipped. Filter may return other errors. ptr must be
  // a *T.
  Filter(ptr interface{}) error
}

// Mapper maps a type T value to a type U value in a Stream.
type Mapper interface {
  // Map does the mapping storing the mapped value at destPtr.
  // If Mapper returns Skipped, then no mapped value is stored at destPtr.
  // Map may return other errors. srcPtr is a *T; destPtr is a *U
  Map(srcPtr interface{}, destPtr interface{}) error
}

// CompositeMapper represents Mappers composed together e.g f(g(x)).
// Programs using CompositeMapper should typically store and pass them as
// values, not pointers. A CompositeMapper can be used by multiple goroutines
// simultaneously if its underlying Mappers can be used by multiple goroutines
// simultaneously. The zero value for CompositeMapper is a Mapper that maps
// nothing (the Map method always returns Skipped).
type CompositeMapper struct {
  _pieces []compositeMapperPiece
}

func (c CompositeMapper) Map(srcPtr interface{}, destPtr interface{}) error {
  return c.Fast().Map(srcPtr, destPtr)
}

// Fast returns a quicker version of this CompositeMapper that cannot be
// used by multiple goroutines simultaneously as if FastCompose were used.
func (c CompositeMapper) Fast() Mapper {
  pieces := c.pieces()
  fastPieces := make([]fastMapperPiece, len(pieces))
  for i := range fastPieces {
    fastPieces[i].setFromCompositePiece(&pieces[i])
  }
  return fastCompositeMapper{fastPieces}
}

func (c CompositeMapper) pieces() []compositeMapperPiece {
  if len(c._pieces) == 0 {
    return nilPieceL
  }
  return c._pieces
}

// Creater of T creates a new, pre-initialized, T and returns a pointer to it.
type Creater func() interface {}

// Copier of T copies the value at src to the value at dest. This type is
// often needed when values of type T need to be pre-initialized. src and
// dest are of type *T and both point to pre-initialized T.
type Copier func(src, dest interface{})

// Rows represents rows in a database table. Most database API already have
// a type that implements this interface
type Rows interface {
  // Next advances to the next row. Next returns false if there is no next row.
  // Every call to Scan, even the first one, must be preceded by a call to Next.
  Next() bool
  // Reads the values out of the current row. args are pointer types.
  Scan(args ...interface{}) error
}

// NilStream returns a Stream that emits no values.
func NilStream() Stream {
  return nilS
}

// Map applies f, which maps a type T value to a type U value, to a Stream
// of T producing a new Stream of U. If s is
// (x1, x2, x3, ...), Map returns the Stream (f(x1), f(x2), f(x3), ...).
// If f returns false for a T value, then the corresponding U value is left
// out of the returned stream. ptr is a *T providing storage for emitted values
// from s. Calling Close on returned Stream closes s. If f is a
// CompositeMapper, Fast() is called on it automatically.
func Map(f Mapper, s Stream, ptr interface{}) Stream {
  ms, ok := s.(*mapStream)
  if ok {
    return &mapStream{FastCompose(f, ms.mapper, ptr), ms.Stream, ms.ptr}
  }
  cm, ok := f.(CompositeMapper)
  if ok {
    return &mapStream{cm.Fast(), s, ptr}
  }
  return &mapStream{f, s, ptr}
}

// Filter filters values from s, returning a new Stream of T. The returned
// Stream's Next method reports any errors besides Skipped that the Filter
// method of f returns. Calling Close on returned Stream closes s.
// f is a Filterer of T; s is a Stream of T.
func Filter(f Filterer, s Stream) Stream {
  fs, ok := s.(*filterStream)
  if ok {
    return &filterStream{All(fs.filterer, f), fs.Stream}
  }
  return &filterStream{f, s}
}

// Count returns an infinite Stream of int which emits all values beginning
// at 0.
func Count() Stream {
  return &count{0, 1}
}

// CountFrom returns an infinite Stream of int emitting values beginning at
// start and increasing by step.
func CountFrom(start, step int) Stream {
  return &count{start, step}
}

// Slice returns a Stream that will emit elements in s starting at index start
// and continuing to but not including index end. Indexes are 0 based. If end
// is negative, it means go to the end of s. Calling Close on returned Stream
// closes s. When end of returned Stream is reached, it closes s if it has not
// consumed s returning any Close error through Next.
func Slice(s Stream, start int, end int) Stream {
  return &sliceStream{Stream: s, start: start, end: end}
}

// ReadRows returns the rows in a database table as a Stream of Tuple. When
// end of returned Stream is reached, it closes r if r implements io.Closer
// propagating any Close error through Next. Calling Close on returned
// stream closes r if r implements io.Closer.
func ReadRows(r Rows) Stream {
  c, _ := r.(io.Closer)
  return &rowStream{rows: r, closer: c}
}

// ReadLines returns the lines of text in r separated by either "\n" or "\r\n"
// as a Stream of string. The emitted string types do not contain the
// end of line characters. When end of returned Stream is reached, it closes
// r if r implements io.Closer propagating any Close error through Next.
// Calling Close on returned Stream closes r if r implements io.Closer.
func ReadLines(r io.Reader) Stream {
  c, _ := r.(io.Closer)
  return &lineStream{bufio: bufio.NewReader(r), closer: c}
}

// Deferred returns a Stream that emits the values from the Stream f returns.
// f is not called until the first time Next is called on the returned stream.
// Calling Close on returned Stream closes the Stream f creates or does nothing
// if f not called.
func Deferred(f func() Stream) Stream {
  return &deferredStream{f: f}
}

// Cycle returns a Stream that repeatedly calls f and emits the resulting
// values. Note that if f repeatedly returns the NilStream, calling Next() on
// returned Stream will create an infinite loop. Calling Close on returned
// Stream closes the last Stream f created or does nothing if f not called. 
// If f returns a Stream of T then Cycle also returns a Stream of T.
func Cycle(f func() Stream) Stream {
  return &cycleStream{Stream: nilS, f: f}
}

// Concat concatenates multiple Streams into one.
// If x = (x1, x2, ...) and y = (y1, y2, ...) then
// Concat(x, y) = (x1, x2, ..., y1, y2, ...).
// Calling Close on returned Stream closes all underlying streams.
// If caller passes a slice to Concat, no copy is made of it.
func Concat(s ...Stream) Stream {
  return &concatStream{s: s}
}

// NewStreamFromValues converts a []T into a Stream of T. aSlice is a []T.
// c is a Copier of T. If c is nil, regular assignment is used.
// Calling Close on returned Stream does nothing.
func NewStreamFromValues(aSlice interface{}, c Copier) Stream {
  sliceValue := getSliceValue(aSlice)
  return &plainStream{sliceValue: sliceValue, copyFunc: toSliceValueCopier(c)}
}

// NewStreamFromPtrs converts a []*T into a Stream of T. aSlice is a []*T.
// c is a Copier of T. If c is nil, regular assignment is used.
// Calling Close on returned Stream does nothing.
func NewStreamFromPtrs(aSlice interface{}, c Copier) Stream {
  sliceValue := getSliceValue(aSlice)
  valueCopierFunc := toSliceValueCopier(c)
  copyFunc := func(src reflect.Value, dest interface{}) {
    valueCopierFunc(reflect.Indirect(src), dest)
  }
  return &plainStream{sliceValue: sliceValue, copyFunc: copyFunc}
}

// Flatten converts a Stream of Stream of T into a Stream of T.
// Calling Close on returned Stream closes s and the last emitted Stream
// from s.
func Flatten(s Stream) Stream {
  return &flattenStream{stream: s, current: nilS}
}

// TakeWhile returns a Stream that emits the values in s until the Filter
// method of f returns Skipped. The returned Stream's Next method reports
// any errors besides Skipped that the Filter method of f returns. When
// end of returned Stream is reached, it automatically closes s if s is
// not exhausted. Calling Close on returned Stream closes s.
// f is a Filterer of T; s is a Stream of T.
func TakeWhile(f Filterer, s Stream) Stream {
  return &takeStream{Stream: s, f: f}
}

// DropWhile returns a Stream that emits the values in s starting at the
// first value where the Filter method of f returns Skipped. The returned
// Stream's Next method reports any errors that the Filter method of f
// returns until it returns Skipped. Calling Close on returned Stream
// closes s. f is a Filterer of T; s is a Stream of T.
func DropWhile(f Filterer, s Stream) Stream {
  return &dropStream{Stream: s, f: f}
}

// Any returns a Filterer that returns Skipped if all of the fs return
// Skipped. Otherwise it returns nil or the first error not equal to Skipped.
func Any(fs ...Filterer) Filterer {
  if len(fs) == 0 {
    return falseFilterer
  }
  ors := make([][]Filterer, len(fs))
  for i := range fs {
    ors[i] = orList(fs[i])
  }
  return orFilterer(filterFlatten(ors))
}

// All returns a Filterer that returns nil if all of the
// fs return nil. Otherwise it returns the first error encountered.
func All(fs ...Filterer) Filterer {
  if len(fs) == 0 {
    return trueFilterer
  }
  ands := make([][]Filterer, len(fs))
  for i := range fs {
    ands[i] = andList(fs[i])
  }
  return andFilterer(filterFlatten(ands))
}

// Compose composes two Mappers together into one e.g f(g(x)). If g maps
// type T values to type U values, and f maps type U values to type V
// values, then Compose returns a CompositeMapper mapping T values to V values.
// c is a Creater of U. Each time Map is called on returned CompositeMapper,
// it invokes c to create a U value to receive the intermediate result from g.
func Compose(f Mapper, g Mapper, c Creater) CompositeMapper {
  l := mapperLen(f) + mapperLen(g)
  pieces := make([]compositeMapperPiece, l)
  n := appendMapper(pieces, g)
  pieces[n - 1].creater = c
  appendMapper(pieces[n:], f)
  return CompositeMapper{pieces}
}

// FastCompose works like Compose except that it uses a *U value instead of
// a Creater of U to link f ang g. ptr is the *U value. Intermediate results
// from g are stored at ptr. Unlike Compose, the Mapper that FastCompose
// returns cannot be used by multiple goroutines simultaneously since what
// ptr points to changes with each call to Map.
func FastCompose(f Mapper, g Mapper, ptr interface{}) Mapper {
  l := mapperLen(f) + mapperLen(g)
  pieces := make([]fastMapperPiece, l)
  n := appendFastMapper(pieces, g)
  pieces[n - 1].ptr = ptr
  appendFastMapper(pieces[n:], f)
  return fastCompositeMapper{pieces}
}

// NewFilterer returns a new Filterer of T. f takes a *T returning nil
// if T value pointed to it should be included or Skipped if it should not
// be included. f can return other errors too.
func NewFilterer(f func(ptr interface{}) error) Filterer {
  return funcFilterer(f)
}

// NewMapper returns a new Mapper mapping T values to U Values. In f,
// srcPtr is a *T and destPtr is a *U pointing to pre-allocated T and U
// values respectively. f returns Skipped if mapped value should be
// skipped. f can also return other errors.
func NewMapper(m func(srcPtr interface{}, destPtr interface{}) error) Mapper {
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
    if err = s.mapper.Map(s.ptr, ptr); err != Skipped {
      return err
    }
  }
  return err
}

type nilStream struct {
}

func (s nilStream) Next(ptr interface{}) error {
  return Done
}

func (s nilStream) Close() error {
  return nil
}

type nilMapper struct {
}

func (m nilMapper) Map(srcPtr, destPtr interface{}) error {
  return Skipped
}

type filterStream struct {
  filterer Filterer
  Stream
}

func (s *filterStream) Next(ptr interface{}) error {
  err := s.Stream.Next(ptr)
  for ; err == nil; err = s.Stream.Next(ptr) {
    if ferr := s.filterer.Filter(ptr); ferr != Skipped {
      return ferr
    }
  }
  return err
}

type sliceStream struct {
  Stream
  start int
  end int
  index int
  done bool
}

func (s *sliceStream) Next(ptr interface{}) error {
  if s.done {
    return Done
  }
  for s.end < 0 || s.index < s.end {
    err := s.Stream.Next(ptr)
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
  s.done = true
  return finish(s.Close())
}

type rowStream struct {
  rows Rows
  closer io.Closer
  done bool
}

func (s *rowStream) Next(ptr interface{}) error {
  if s.done {
    return Done
  }
  if !s.rows.Next() {
    s.done = true
    return finish(s.Close())
  }
  ptrs := ptr.(Tuple).Ptrs()
  return s.rows.Scan(ptrs...)
}

func (s *rowStream) Close() error {
  return closeUnder(&s.closer)
}
  
type lineStream struct {
  bufio *bufio.Reader
  closer io.Closer
  done bool
}

func (s *lineStream) Next(ptr interface{}) error {
  if s.done {
    return Done
  }
  p := ptr.(*string)
  line, isPrefix, err := s.bufio.ReadLine()
  if err == io.EOF {
    s.done = true
    return finish(s.Close())
  }
  if err != nil {
    return err
  }
  if !isPrefix {
    *p = string(line)
    return nil
  }
  *p, err = s.readRestOfLine(line)
  return err
}

func (s *lineStream) readRestOfLine(line []byte) (string, error) {
  lines := [][]byte{copyBytes(line)}
  for {
    l, isPrefix, err := s.bufio.ReadLine()
    if err == io.EOF {
      break
    }
    if err != nil {
      return "", err
    }
    lines = append(lines, copyBytes(l))
    if !isPrefix {
      break
    }
  }
  return string(byteFlatten(lines)), nil
}

func (s *lineStream) Close() error {
  return closeUnder(&s.closer)
}

type deferredStream struct {
  f func() Stream
  s Stream
  done bool
}

func (d *deferredStream) Next(ptr interface{}) error {
  if d.done {
    return Done
  }
  if d.s == nil {
    d.s = d.f()
  }
  err := d.s.Next(ptr)
  if err == Done {
    d.done = true
    d.s = nil
  }
  return err
}

func (d *deferredStream) Close() error {
  if d.s != nil {
    return d.s.Close()
  }
  return nil
}

type cycleStream struct {
  Stream
  f func() Stream
}

func (c *cycleStream) Next(ptr interface{}) error {
  err := c.Stream.Next(ptr)
  for ; err == Done; err = c.Stream.Next(ptr) {
    c.Stream = c.f()
  }
  return err
}

type concatStream struct {
  s []Stream
  idx int
}

func (c *concatStream) Next(ptr interface{}) error {
  for ;c.idx < len(c.s); c.idx++ {
    err := c.s[c.idx].Next(ptr)
    if err == Done {
      continue
    }
    return err
  }
  return Done
}

func (c *concatStream) Close() error {
  var result error
  for i := range c.s {
    err := c.s[i].Close()
    if result == nil {
      result = err
    }
  }
  return result
}

type plainStream struct {
  sliceValue reflect.Value
  copyFunc func(src reflect.Value, dest interface{})
  index int
}

func (s *plainStream) Next(ptr interface{}) error {
  if s.index == s.sliceValue.Len() {
    return Done
  }
  s.copyFunc(s.sliceValue.Index(s.index), ptr)
  s.index++
  return nil
}

func (s *plainStream) Close() error {
  return nil
}

type flattenStream struct {
  stream Stream
  current Stream
}

func (s *flattenStream) Next(ptr interface{}) error {
  err := s.current.Next(ptr)
  for ; err == Done; err = s.current.Next(ptr) {
    var temp Stream
    serr := s.stream.Next(&temp)
    if serr != nil {
      return serr
    }
    s.current = temp
  }
  return err
}

func (s *flattenStream) Close() error {
  result := s.current.Close()
  err := s.stream.Close()
  if result == nil {
    result = err
  }
  return result
}

type takeStream struct {
  Stream
  f Filterer
}

func (s *takeStream) Next(ptr interface{}) error {
  if s.f == nil {
    return Done
  }
  err := s.Stream.Next(ptr)
  if err == Done {
    s.f = nil
    return Done
  }
  if err != nil {
    return err
  }
  if ferr := s.f.Filter(ptr); ferr != Skipped {
    return ferr
  }
  s.f = nil
  return finish(s.Close())
}

type dropStream struct {
  Stream
  f Filterer
}

func (s *dropStream) Next(ptr interface{}) error {
  err := s.Stream.Next(ptr)
  if s.f == nil {
    return err
  }
  for ; err == nil; err = s.Stream.Next(ptr) {
    ferr := s.f.Filter(ptr)
    if ferr == Skipped {
      s.f = nil
      return nil
    }
    if ferr != nil {
      return ferr
    }
  }
  return err
}
  
type funcFilterer func(ptr interface{}) error

func (f funcFilterer) Filter(ptr interface{}) error {
  return f(ptr)
}

type andFilterer []Filterer

func (f andFilterer) Filter(ptr interface{}) error {
  for i := range f {
    if err := f[i].Filter(ptr); err != nil {
      return err
    }
  }
  return nil
}

type orFilterer []Filterer

func (f orFilterer) Filter(ptr interface{}) error {
  for i := range f {
    if err := f[i].Filter(ptr); err != Skipped {
      return err
    }
  }
  return Skipped
}

type funcMapper func(srcPtr interface{}, destPtr interface{}) error

func (m funcMapper) Map(srcPtr interface{}, destPtr interface{}) error {
  return m(srcPtr, destPtr)
}

type fastCompositeMapper struct {
  pieces []fastMapperPiece
}

func (m fastCompositeMapper) Map(srcPtr interface{}, destPtr interface{}) error {
  sPtr := srcPtr
  var dPtr interface{}
  length := len(m.pieces)
  for i := range m.pieces {
    piece := &m.pieces[i]
    if (i == length - 1) {
      dPtr = destPtr
    } else {
      dPtr = piece.ptr
    }
    if err := piece.mapper.Map(sPtr, dPtr); err != nil {
      return err
    }
    sPtr = dPtr
  }
  return nil
}

type compositeMapperPiece struct {
  mapper Mapper
  creater Creater
}

func (cmp *compositeMapperPiece) setFromFastPiece(fmp *fastMapperPiece) {
  cmp.mapper = fmp.mapper
  if fmp.ptr == nil {
    cmp.creater = nil
  } else {
    cmp.creater = newCreater(fmp.ptr)
  }
}

type fastMapperPiece struct {
  mapper Mapper
  ptr interface{}
}

func (fmp *fastMapperPiece) setFromCompositePiece(cmp *compositeMapperPiece) {
  fmp.mapper = cmp.mapper
  if cmp.creater == nil {
    fmp.ptr = nil
  } else {
    fmp.ptr = cmp.creater()
  }
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
  switch am := m.(type) {
  case CompositeMapper:
    return len(am.pieces())
  case fastCompositeMapper:
    return len(am.pieces)
  }
  return 1
}

func appendMapper(pieces []compositeMapperPiece, m Mapper) int {
  switch am := m.(type) {
  case CompositeMapper:
    return copy(pieces, am.pieces())
  case fastCompositeMapper:
    for i := range am.pieces {
      pieces[i].setFromFastPiece(&am.pieces[i])
    }
    return len(am.pieces)
  default:
    pieces[0] = compositeMapperPiece{mapper: m}
  }
  return 1
}

func appendFastMapper(pieces []fastMapperPiece, m Mapper) int {
  switch am := m.(type) {
  case CompositeMapper:
    ampieces := am.pieces()
    for i := range ampieces {
      pieces[i].setFromCompositePiece(&ampieces[i])
    }
    return len(ampieces)
  case fastCompositeMapper:
    return copy(pieces, am.pieces)
  default:
    pieces[0] = fastMapperPiece{mapper: m}
  }
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

func closeUnder(ptr *io.Closer) error {
  if *ptr == nil {
    return nil
  }
  result := (*ptr).Close()
  if result == nil {
    *ptr = nil
  }
  return result
}

func copyBytes(b []byte) []byte {
  result := make([]byte, len(b))
  copy(result, b)
  return result
}

func byteFlatten(b [][]byte) []byte {
  var l int
  for i := range b {
    l += len(b[i])
  }
  result := make([]byte, l)
  n := 0
  for i := range b {
    n += copy(result[n:], b[i])
  }
  return result
}

func toSliceValueCopier(c Copier) func(src reflect.Value, dest interface{}) {
  if c == nil {
    return assignFromValue
  }
  return func(src reflect.Value, dest interface{}) {
    c(src.Addr().Interface(), dest)
  }
}

func assignCopier(src, dest interface{}) {
  srcP := reflect.ValueOf(src)
  assignFromValue(reflect.Indirect(srcP), dest)
}

func assignFromValue(src reflect.Value, dest interface{}) {
  destP := reflect.ValueOf(dest)
  reflect.Indirect(destP).Set(src)
}

func getSliceValue(aSlice interface{}) reflect.Value {
  sliceValue := reflect.ValueOf(aSlice)
  if sliceValue.Kind() != reflect.Slice {
    panic("Slice argument expected")
  }
  return sliceValue
}

