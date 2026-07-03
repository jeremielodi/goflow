package repository

import (
	"testing"
	"time"
)

func TestJobWakeChannel_NotifyWakesExistingWaiter(t *testing.T) {
	jobType := "wake-test-" + t.Name()
	wake := JobWakeChannel(jobType)

	woke := make(chan struct{})
	go func() {
		<-wake
		close(woke)
	}()

	// Give the goroutine a moment to start waiting before notifying.
	time.Sleep(10 * time.Millisecond)
	NotifyJobAvailable(jobType)

	select {
	case <-woke:
	case <-time.After(time.Second):
		t.Fatal("waiter was not woken within 1s of NotifyJobAvailable")
	}
}

func TestJobWakeChannel_UnrelatedTypeIsNotWoken(t *testing.T) {
	watched := "wake-test-watched-" + t.Name()
	other := "wake-test-other-" + t.Name()
	wake := JobWakeChannel(watched)

	NotifyJobAvailable(other)

	select {
	case <-wake:
		t.Fatal("wake channel for a different job type fired")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestJobWakeChannel_FreshChannelAfterNotify(t *testing.T) {
	jobType := "wake-test-fresh-" + t.Name()
	first := JobWakeChannel(jobType)
	NotifyJobAvailable(jobType)

	select {
	case <-first:
	default:
		t.Fatal("expected the pre-notify channel to be closed")
	}

	second := JobWakeChannel(jobType)
	select {
	case <-second:
		t.Fatal("expected a fresh, still-open channel to be installed after notify")
	default:
	}
}
