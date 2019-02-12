package logging

import (
	"bytes"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-logfmt/logfmt"
)

type deduper struct {
	next   log.Logger
	repeat time.Duration

	mtx  sync.RWMutex
	seen map[string]time.Time
}

// Dedupe log lines to next, only repeating every repeat duration.
func Dedupe(next log.Logger, repeat time.Duration) log.Logger {
	return &deduper{
		next:   next,
		repeat: repeat,
		seen:   map[string]time.Time{},
	}
}

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

func encode(keyvals ...interface{}) (string, error) {
	enc := logfmtEncoderPool.Get().(*logfmtEncoder)
	enc.Reset()
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

func (d *deduper) Log(keyvals ...interface{}) error {
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
