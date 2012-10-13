// This example is just like power, but does not make recursive calls to
// functional.NewGenerator. functional.NewGenerator uses channels to simulate
// the behavior of python generators. While channels are cheap in go, they
// are not free. As a result, this example runs 10X faster than the power
// example.
package main

import (
  "fmt"
  "github.com/keep94/gofunctional2/functional"
)

var ess = emptySetStream{}

// Power returns a Stream that emits the power set of items. Next of 
// returned Stream emits to an []int that has same length as items.
func Power(items []int) functional.Stream {
  len := len(items)
  if len == 0 {
    return functional.Slice(ess, 0, 1)
  }
  return functional.Concat(
      Power(items[:len-1]),
      functional.Deferred(func() functional.Stream {
          return functional.Filter(
              appendFilterer(items[len-1]),
              Power(items[:len-1]))
      }))
}

type emptySetStream struct {
}

func (s emptySetStream) Next(ptr interface{}) error {
  p := ptr.(*[]int)
  *p = (*p)[:0]
  return nil
}

func (s emptySetStream) Close() error {
  return nil
}

// appendFilterer adds a particular int to an existing set.
type appendFilterer int

func (a appendFilterer) Filter(ptr interface{}) bool {
  p := ptr.(*[]int)
  *p = append(*p, int(a))
  return true
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
