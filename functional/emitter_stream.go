package functional

type emitterStream struct {
  errCh chan error
  ptrCh chan interface{}
  ptr interface{}
}

func (s *emitterStream) Next(ptr interface{}) error {
  s.ptrCh <- ptr
  return <-s.errCh
}

func (s *emitterStream) Return(err error) {
  s.errCh <- err
  s.ptr = <-s.ptrCh
}

func (s *emitterStream) EmitPtr() interface{} {
  return s.ptr
}

func (s *emitterStream) startEmitter() {
  s.ptr = <-s.ptrCh
}

func (s *emitterStream) endEmitter() {
  s.errCh <- Done
}

func (s *emitterStream) startStream() {
  <-s.errCh
}

func (s *emitterStream) endStream() {
  s.ptrCh <- nil
}

func (s *emitterStream) close() {
  close(s.errCh)
  s.errCh = nil
  close(s.ptrCh)
  s.ptrCh = nil
}

func (s *emitterStream) isClosed() bool {
  return s.ptrCh == nil
}
