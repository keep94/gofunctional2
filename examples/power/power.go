// This program demonstrates creating a Stream that emits the elements in the
// power set of an arbitrarily large set.
package main

import (
  "fmt"
  "github.com/keep94/gofunctional2/functional"
)

// Power returns a Stream that emits the power set of items. Next of 
// returned Stream emits to an []int that has same length as items.
func Power(items []int) functional.Stream {
  return functional.NewGenerator(func(e functional.Emitter) {
    len := len(items)
    if len == 0 {
      ptr := e.EmitPtr()
      if ptr != nil {
        p := ptr.(*[]int)
        *p = (*p)[:0]
        e.Return(nil)
      }
      return
    }
    if functional.EmitAll(Power(items[:len-1]), e) != nil {
      return
    }
    functional.EmitAll(functional.Filter(appendFilterer(items[len-1]), Power(items[:len-1])), e)
  })
}

// appendFilterer adds a particular int to an existing set.
type appendFilterer int

func (a appendFilterer) Filter(ptr interface{}) error {
  p := ptr.(*[]int)
  *p = append(*p, int(a))
  return nil
}

func main() {
  orig := make([]int, 100)
  for i := range orig {
    orig[i] = i
  }

  // Return the 10000th up to the 10010th element of the power set of
  // {0, 1, .. 99}.
  // This entire power set would have 2^100 elements in it!
  s := functional.Slice(Power(orig), 10000, 10010)
  result := make([]int, len(orig))
  for s.Next(&result) == nil {
    fmt.Println(result)
  }
}
