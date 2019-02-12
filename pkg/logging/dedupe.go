package logging

import (
	"bytes"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-logfmt/logfmt"
)

const (
	garbageCollectEvery = 10 * time.Second
	expireEntriesAfter  = 1 * time.Minute
)

type logfmtEncoder struct {
	*logfmt.Encoder
	buf bytes.Buffer
}

var logfmtEncoderPool = sync.Pool{
	New: func() interface{} {
		var enc logfmtEncoder
		enc.Encoder = logfmt.NewEncoder(&enc.buf)
		return &enc
	},
}

// Deduper implement log.Logger, dedupes log lines.
type Deduper struct {
	next   log.Logger
	repeat time.Duration
	quit   chan struct{}
	mtx    sync.RWMutex
	seen   map[string]time.Time
}

// Dedupe log lines to next, only repeating every repeat duration.
func Dedupe(next log.Logger, repeat time.Duration) *Deduper {
	d := &Deduper{
		next:   next,
		repeat: repeat,
		quit:   make(chan struct{}),
		seen:   map[string]time.Time{},
	}
	go d.run()
	return d
}

// Stop the Deduper.
func (d *Deduper) Stop() {
	close(d.quit)
}

func (d *Deduper) run() {
	ticker := time.NewTicker(garbageCollectEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.mtx.Lock()
			now := time.Now()
			for line, seen := range d.seen {
				if now.Sub(seen) > expireEntriesAfter {
					delete(d.seen, line)
				}
			}
			d.mtx.Unlock()
		case <-d.quit:
			return
		}
	}
}

// Log implements log.Logger.
func (d *Deduper) Log(keyvals ...interface{}) error {
	line, err := encode(keyvals...)
	if err != nil {
		return err
	}

	d.mtx.RLock()
	last, ok := d.seen[line]
	d.mtx.RUnlock()

	if ok && time.Since(last) < d.repeat {
		return nil
	}

	d.mtx.Lock()
	d.seen[line] = time.Now()
	d.mtx.Unlock()

	return d.next.Log(keyvals...)
}

func encode(keyvals ...interface{}) (string, error) {
	enc := logfmtEncoderPool.Get().(*logfmtEncoder)
	enc.buf.Reset()
	defer logfmtEncoderPool.Put(enc)

	if err := enc.EncodeKeyvals(keyvals...); err != nil {
		return "", err
	}

	// Add newline to the end of the buffer
	if err := enc.EndRecord(); err != nil {
		return "", err
	}

	return string(enc.buf.Bytes()), nil
}
