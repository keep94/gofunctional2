// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

// checkbook is a small program that prints a checkbook register from a
// database. It first reads from the database the balance of the account.
// Then it prints the entries in the account showing the balance at each
// transaction. It also prints amount of total deposits and withdrawals.
//
// When this program is run, the current working directory needs to be this
// directory or else the program will not find the sqlite file, chkbook.db
package main

import (
  "code.google.com/p/gosqlite/sqlite"
  "errors"
  "fmt"
  "github.com/keep94/gofunctional2/functional"
)

// Entry represents an entry in a checkbook register
type Entry struct {
  // YYYYmmdd format
  Date string
  Name string
  // $40.64 is 4064
  Amount int64
  // Balance is the remaining balance in account. $40.64 is 4064
  Balance int64
}

func (e *Entry) String() string {
  return fmt.Sprintf("date: %s; name: %s; amount: %d; balance: %d", e.Date, e.Name, e.Amount, e.Balance)
}

func (e *Entry) Ptrs() []interface{} {
  return []interface{} {&e.Date, &e.Name, &e.Amount}
}
  
// ChkbookEntries returns a Stream that emits all the entries in a
// checkbook ordered by most recent to least recent. conn is the sqlite
// connection; acctId is the id of the account for which to print entries.
// If acctId does not match a valid account, ChkbookEntries will return an
// error and nil for the Stream. If caller does not exhaust returned
// Stream, it must call Close on it to free up resources.
func ChkbkEntries(conn *sqlite.Conn, acctId int) (functional.Stream, error) {
  stmt, err := conn.Prepare("select balance from balances where acct_id = ?")
  if err != nil {
   return nil, err
  }
  if err = stmt.Exec(acctId); err != nil {
    stmt.Finalize()
    return nil, err
  }
  if !stmt.Next() {
    stmt.Finalize()
    return nil, errors.New("No balance")
  }
  var bal int64
  if err = stmt.Scan(&bal); err != nil {
    stmt.Finalize()
    return nil, err
  }
  stmt.Finalize()
  stmt, err = conn.Prepare("select date, name, amount from entries where acct_id = ? order by date desc")
  if err != nil {
    return nil, err
  }
  if err = stmt.Exec(acctId); err != nil {
    stmt.Finalize()
    return nil, err
  }
  rowStream := functional.ReadRows(CloserStmt{stmt})
  return functional.Filter(&BalanceFilterer{bal}, rowStream), nil
}

// CloserStmt makes a sqlite statement implement io.Closer so that Streams
// will close it automatically.
type CloserStmt struct {
  *sqlite.Stmt
}

func (c CloserStmt) Close() error {
  return c.Stmt.Finalize()
}

// BalanceFilterer adds Balances to checkbook entries. A BalanceFilterer
// value is only good for one pass.
type BalanceFilterer struct {
  // Balance should be set to the ending balance initially.
  Balance int64
}

func (b *BalanceFilterer) Filter(ptr interface{}) error {
  p := ptr.(*Entry)
  p.Balance = b.Balance
  b.Balance += p.Amount
  return nil
}

// Printer prints entries to stdout
type Printer struct {
}

// Consume prints all Entry values that s emits to stdout. On error, prints
// the error encountered and halts.
func (p Printer) Consume(s functional.Stream) {
  var entry Entry
  err := s.Next(&entry)
  for ; err == nil; err = s.Next(&entry) {
    fmt.Println(&entry)
  }
  if err != functional.Done {
    fmt.Printf("Error happened: %v\n", err)
    s.Close()
  }
}

// Totaler totals expenses or incomes. Totaler value is only good for
// one pass.
type Totaler struct {
  // Total is the computed total. Should be 0 initially.
  Total int64
  // If Income is true, total incomes; otherwise total expenses.
  Income bool
}

// Consume computes the total of all income Entry values that s emits if
// Income is true; otherwise, it computes the total of all expense Entry
// values s emits.
func (t *Totaler) Consume(s functional.Stream) {
  var entry Entry
  err := s.Next(&entry)
  for ; err == nil; err = s.Next(&entry) {
    if t.Income {
      if entry.Amount < 0 {
        t.Total -= entry.Amount
      }
    } else {
      if entry.Amount > 0 {
        t.Total += entry.Amount
      }
    }
  }
  if err != functional.Done {
    s.Close()
  }
}

func main() {
  conn, err := sqlite.Open("chkbook.db")
  if err != nil {
    fmt.Println("Error opening file")
    return
  }
  s, err := ChkbkEntries(conn, 1)
  if err != nil {
    fmt.Printf("Error reading ledger %v", err)
  }
  expenseTotaler := &Totaler{}
  incomeTotaler := &Totaler{Income: true}
  printer := Printer{}
  err = functional.MultiConsume(s, new(Entry), nil, printer, expenseTotaler, incomeTotaler)
  if err != nil {
    fmt.Printf("Encountered error closing stream: %v", err)
  }
  fmt.Printf("Total income: %d\n", incomeTotaler.Total)
  fmt.Printf("Total expenses: %d\n", expenseTotaler.Total)
}
