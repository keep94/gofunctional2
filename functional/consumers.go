package functional

// A Consumer of T consumes the T values from a Stream of T.
type Consumer interface {

  // Consume consumes values from Stream s
  Consume(s Stream)
}

// ModifyConsumerStream returns a new Consumer that applies f to its Stream
// and then gives the result to c. If c is a Consumer of T and f takes a
// Stream of U and returns a Stream of T, then ModifyConsumerStream returns a
// Consumer of U.
func ModifyConsumerStream(c Consumer, f func(s Stream) Stream) Consumer {
  return &modifiedConsumerStream{c, f}
}

// MultiConsume consumes the values of s, a Stream of T, sending those T
// values to each Consumer in consumers. MultiConsume consumes values from s
// until no Consumer in consumers is accepting values.
// ptr is a *T that receives the values from s. copier is a Copier
// of T used to copy T values to the Streams sent to each Consumer in
// consumers. Passing null for copier means use simple assignment.
// Finally MultiConsume closes s and returns the result.
func MultiConsume(s Stream, ptr interface{}, copier Copier, consumers ...Consumer) error {
  if copier == nil {
    copier = assignCopier
  }
  streams := make([]splitStream, len(consumers))
  for i := range streams {
    streams[i] = splitStream{emitterStream{ptrCh: make(chan interface{}), errCh: make(chan error)}}
    go func(s *splitStream, c Consumer) {
      defer s.endStream()
      s.startStream()
      c.Consume(s)
    }(&streams[i], consumers[i])
  }
  var err error
  for asyncReturn(streams, err) {
    err = s.Next(ptr)
    for i := range streams {
      if !streams[i].isClosed() {
        p := streams[i].EmitPtr()
        copier(ptr, p)
      }
    }
  }
  return s.Close()
}

type modifiedConsumerStream struct {
  c Consumer
  f func(s Stream) Stream
}

func (mc *modifiedConsumerStream) Consume(s Stream) {
  mc.c.Consume(mc.f(s))
}

type splitStream struct {
  emitterStream
}

func (s *splitStream) Next(ptr interface{}) error {
  if ptr == nil {
    panic("Got nil pointer in Next.")
  }
  return s.emitterStream.Next(ptr)
}

func (s *splitStream) Close() error {
  return nil
}

func asyncReturn(streams []splitStream, err error) bool {
  for i := range streams {
    if !streams[i].isClosed() {
      streams[i].errCh <- err
    }
  }
  result := false
  for i := range streams {
    if !streams[i].isClosed() {
      streams[i].ptr = <-streams[i].ptrCh
      if streams[i].ptr == nil {
        streams[i].close()
      } else {
        result = true
      }
    }
  }
  return result
}
