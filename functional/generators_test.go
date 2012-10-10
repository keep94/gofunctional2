package functional

import (
    "fmt"
    "testing"
)

func TestNewInfiniteGenerator(t *testing.T) {

  var finished bool
  // fibonacci
  fib := NewGenerator(
      func(e Emitter) {
        a := 0
        b := 1
        for ptr := e.EmitPtr(); ptr != nil; ptr = e.EmitPtr() {
          p := ptr.(*int)
          *p = a
          e.Return(nil)
          a, b = b, a + b
        }
        finished = true
      })
  var results []int
  stream := Slice(fib, 0, 7)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[0 1 1 2 3 5 8]"  {
    t.Errorf("Expected [0 1 1 2 3 5 8] got %v", output)
  }
  if !finished {
    t.Error("Generating function should complete on close.")
  }
  verifyDone(t, stream, new(int), err)
}

func TestNewFiniteGenerator(t *testing.T) {
  var finished bool
  stream := NewGenerator(
      func(e Emitter) {
        values := []int{1, 2, 5}
        for i := range values {
          ptr := e.EmitPtr()
          if ptr == nil {
            break
          }
          *ptr.(*int) = values[i]
          e.Return(nil)
        }
        finished = true
      })
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[1 2 5]" {
    t.Errorf("Expected [1 2 5] got %v", output)
  }
  if !finished {
    t.Error("Generating function should have completed.")
  }
  verifyDone(t, stream, new(int), err)
}

func TestEmptyGenerator(t *testing.T) {
  var finished bool
  stream := NewGenerator(func (e Emitter) { finished = true })
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]" {
    t.Errorf("Expected [] got %v", output)
  }
  if !finished {
    t.Error("Generating function should have completed.")
  }
  verifyDone(t, stream, new(int), err)
}
