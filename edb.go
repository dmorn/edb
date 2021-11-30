package edb

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"
)

type Event struct {
	Id     string
	Issuer string
	Scope  string
	Action string
	Time   time.Time
	Data   []string
}

type File interface {
	fs.File
	io.Writer
	io.Closer
	io.Seeker
}

type Db struct {
	sync.Mutex

	f File
	w *csv.Writer
}

// Append a new event to edb. Safe to use by multiple goroutines.
func (d *Db) Append(e *Event) error {
	fields := append([]string{
		e.Id,
		e.Issuer,
		e.Scope,
		e.Action,
		time.Now().Format(time.RFC3339),
	}, e.Data...)

	d.Lock()
	defer d.Unlock()

	if d.w == nil {
		d.w = csv.NewWriter(d.f)
	}
	if err := d.w.Write(fields); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	d.w.Flush()
	return nil
}

func (d *Db) Find(eid string) (*Event, bool) {
	d.Lock()
	defer d.Unlock()

	var e Event
	var ok bool
	d.Revive(func(next Event) error {
		if next.Id != eid {
			return nil
		}

		e, ok = next, true
		e.Data = make([]string, len(next.Data))
		copy(e.Data, next.Data)
		return nil
	})
	return &e, ok
}

func (d *Db) Dump(w io.Writer) error {
	d.Lock()
	defer d.Unlock()

	d.f.Seek(0, io.SeekStart)
	defer d.f.Seek(0, io.SeekEnd)

	if _, err := io.Copy(w, d.f); err != nil {
		return fmt.Errorf("dump: %w", err)
	}
	return nil
}

func (d *Db) Revive(h func(e Event) error) error {
	d.f.Seek(0, io.SeekStart)
	defer d.f.Seek(0, io.SeekEnd)

	r := csv.NewReader(d.f)
	r.Comment = '#'
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true
	r.ReuseRecord = true

	var e Event
	for line := 1; ; line++ {
		rec, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("revive: %w", err)
		}
		if len(rec) < 4 {
			return fmt.Errorf("revive: line %d: unexpected record %q", line, rec)
		}
		e.Id = rec[0]
		e.Issuer = rec[1]
		e.Scope = rec[2]
		e.Action = rec[3]
		t, err := time.Parse(time.RFC3339, rec[4])
		if err != nil {
			return fmt.Errorf("revive: line %d: %w", line, err)
		}
		e.Time = t
		e.Data = rec[5:]
		if err := h(e); err != nil {
			return fmt.Errorf("revive: %w", err)
		}
	}
}

func (db *Db) Close() error {
	db.Lock()
	defer db.Unlock()

	if w := db.w; w != nil {
		db.w.Flush()
		db.w = nil
	}
	db.f.Close()
	db.f = nil
	return nil
}

func Open(p string) (*Db, error) {
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("from path: %w", err)
	}
	return &Db{f: f}, nil
}
