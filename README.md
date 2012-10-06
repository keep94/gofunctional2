# gofunctional2

Functional programming in go. The main data type, Stream, is similar to
a python iterator or generator. The methods found in here are similar to
the methods found in the python itertools module. This is version 2 of
gofunctional.

## Using

	import "github.com/keep94/gofunctional2/functional"

## Installing

	go get github.com/keep94/gofunctional2

## Real World Example

Suppose there are names and phone numbers of people stored in a sqlite
database. The table has a name and phone_number column.

The person class would look like:

	type Person struct {
	  Name string
	  Phone string
	}

	func (p *Person) Ptrs() {
	  return []interface{}{&p.Name, &p.Phone}
	}

To get the 4th page of 25 people do:

	package main

	import (
	  "code.google.com/p/gosqlite/sqlite"
	  "github.com/keep94/gofunctional2/functional"
	)

	func main() {
	  conn, _ := sqlite.Open("YourDataFilePath")
	  stmt, _ := conn.Prepare("select * from People")
	  s := functional.ReadRows(stmt)
	  s = functional.Slice(s, 3 * 25, 4 * 25)
          var person Person
          err := s.Next(&person)
          for ; err == nil; err = s.Next(&person) {
            // Display person here
          }
          if err != functional.Done {
            // Do error handling here

            s.Close()
          }
        }

Like python iterators and generators, Stream types are lazily evaluated, so
the above code will read only the first 100 names no matter how many people
are in the database.

See tests and the included example for detailed usage.
