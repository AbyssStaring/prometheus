package logging

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type counter int

func (c *counter) Log(keyvals ...interface{}) error {
	(*c)++
	return nil
}

func TestDedupe(t *testing.T) {
	var c counter
	d := Dedupe(&c, 100*time.Millisecond)
	defer d.Stop()

	// Log 10 times quickly, ensure they are deduped.
	for i := 0; i < 10; i++ {
		err := d.Log("msg", "hello")
		require.NoError(t, err)
	}
	require.Equal(t, 1, int(c))

	// Wait, then log again, make sure it is logged.
	time.Sleep(200 * time.Millisecond)
	err := d.Log("msg", "hello")
	require.NoError(t, err)
	require.Equal(t, 2, int(c))
}
