package delay_queue

import (
	"context"
	"github.com/samber/lo"
	"testing"
	"time"
)

func TestSimpleTimeoutHub(t *testing.T) {
	ctx := context.Background()
	timeoutQueueHub := NewTimeoutQueueHub[int](WithTickTime[int](time.Millisecond*2), WithTimeoutCallback(func(ctx context.Context, data int, now *time.Time, delay *time.Time) {
		timeOver := now.Sub(*delay).Milliseconds()
		t.Logf("data: %+v, now: %+v, timeover: %d", data, now, timeOver)
	}))
	timeoutQueueHub.GetQueue(ctx, "1").Push(10, lo.ToPtr(time.Now()))
	time.Sleep(5 * time.Millisecond)
}

func TestSimpleTimeoutHub2(t *testing.T) {
	ctx := context.Background()
	timeoutQueueHub := NewTimeoutQueueHub[int](WithTickTime[int](time.Millisecond*2), WithTimeoutCallback(func(ctx context.Context, data int, now *time.Time, delay *time.Time) {
		timeOver := now.Sub(*delay).Milliseconds()
		if timeOver < int64(1) || timeOver > 2 {
			t.Errorf("data: %+v, now: %+v, timeover: %d", data, now, timeOver)
		}
	}))
	timeoutQueueHub.GetQueue(ctx, "1").Push(10, lo.ToPtr(time.Now().Add(1*time.Millisecond)))
	time.Sleep(5 * time.Millisecond)
}

func TestClearCallback(t *testing.T) {
	ctx := context.Background()
	timeoutQueueHub := NewTimeoutQueueHub[int](WithTickTime[int](time.Millisecond*2), WithTimeoutCallback(func(ctx context.Context, data int, now *time.Time, delay *time.Time) {
		timeOver := now.Sub(*delay).Milliseconds()
		t.Logf("timeout: data: %+v, now: %+v, timeover: %d", data, now, timeOver)
	}), WithClearCallback(func(ctx context.Context, data int) {
		t.Logf("clear: data: %+v", data)
	}))
	timeoutQueueHub.GetQueue(ctx, "1").Push(100, lo.ToPtr(time.Now().Add(1*time.Millisecond)))
	timeoutQueueHub.GetQueue(ctx, "1").Push(101, lo.ToPtr(time.Now().Add(1*time.Millisecond).Add(100*time.Nanosecond)))
	timeoutQueueHub.GetQueue(ctx, "1").Push(102, lo.ToPtr(time.Now().Add(1*time.Millisecond).Add(200*time.Nanosecond)))
	timeoutQueueHub.GetQueue(ctx, "1").Push(103, lo.ToPtr(time.Now().Add(1*time.Millisecond).Add(300*time.Nanosecond)))
	timeoutQueueHub.GetQueue(ctx, "1").Push(104, lo.ToPtr(time.Now().Add(2*time.Millisecond)))
	timeoutQueueHub.GetQueue(ctx, "1").Push(200, lo.ToPtr(time.Now().Add(3*time.Millisecond)))

	time.Sleep(2 * time.Millisecond)
	timeoutQueueHub.GetQueue(ctx, "1").Clear()
	time.Sleep(5 * time.Millisecond)
}
