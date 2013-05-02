// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

// This program solves the following puzzle: Find the nth digit of the
// sequence 12345678910111213141516... To find the 287th digit, run "puzzle 287"
package main

import (
  "flag"
  "fmt"
  "github.com/keep94/gofunctional2/functional"
  "strconv"
)

// Emits 01234567891011121314151617... as a Stream of runes.
func AllDigits() functional.Stream {
  return functional.NewGenerator(
      func(e functional.Emitter) {
        for number := 0; ; number++ {
          for _, ch := range strconv.Itoa(number) {
            ptr := e.EmitPtr()
            if ptr == nil {
              return
            }
            *(ptr.(*rune)) = ch
            e.Return(nil)
          }
        }
      })
}

// Return the nth digit of 1234567891011121314151617....
func Digit(posit int) string {
  s := functional.Slice(AllDigits(), posit, -1)
  var r rune
  s.Next(&r)
  s.Close()
  return string(r)
}

func main() {
  flag.Parse()
  posit, _ := strconv.Atoi(flag.Arg(0))
  fmt.Println(Digit(posit))
}
