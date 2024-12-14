// Package completion provides a thread-safe mechanism for signaling completion
// similar to context.Done() but with explicit completion control.
package completion

import (
	"sync"
	"sync/atomic"
)

// Completer provides a thread-safe completion mechanism.
type Completer struct {
	mu       sync.Mutex
	done     chan struct{}
	complete atomic.Bool
}

// New creates a new Completer instance.
func New() *Completer {
	return &Completer{
		done: make(chan struct{}),
	}
}

// Complete marks the Completer as complete and closes the Done channel.
// It is safe to call Complete multiple times; only the first call will have an effect.
func (c *Completer) Complete() {
	if c.complete.CompareAndSwap(false, true) {
		c.mu.Lock()
		defer c.mu.Unlock()
		select {
		case <-c.done:
			// Already closed
		default:
			close(c.done)
		}
	}
}

// Reset resets the Completer to its initial uncompleted state.
// It is safe to call Reset concurrently with other methods.
// Any goroutines waiting on the previous Done() channel will receive a close signal.
func (c *Completer) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If it wasn't already completed, no need to reset
	if !c.complete.Swap(false) {
		return
	}

	// Close old channel if it's not already closed
	select {
	case <-c.done:
		// Already closed
	default:
		close(c.done)
	}

	// Create new channel
	c.done = make(chan struct{})
}

// Done returns a channel that will be closed when Complete is called.
// Multiple goroutines can safely read from this channel.
// After Reset(), this returns a new channel.
func (c *Completer) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// IsComplete returns true if Complete has been called and no Reset has occurred since,
// false otherwise.
func (c *Completer) IsComplete() bool {
	return c.complete.Load()
}

// Wait blocks until Complete is called.
// If Reset is called while waiting, Wait will continue to block until the next Complete.
func (c *Completer) Wait() {
	<-c.Done()
}
