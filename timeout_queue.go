package delay_queue

import (
	"context"
	"time"
)

type timeoutCallback[T any] func(ctx context.Context, data T, now *time.Time, delay *time.Time)
type clearCallback[T any] func(ctx context.Context, data T)

const queueItemTimeoutTopicName = "topic:handleQueueItemTimeout"

type TimeoutQueue[T any] struct {
	ctx                 context.Context
	dp                  *DelayQueue[T]
	tickTime            time.Duration
	timeoutCallBackFunc timeoutCallback[T]
	clearCallBackFunc   clearCallback[T]
}

func newTimeoutQueue[T any](ctx context.Context, tickTime time.Duration, timeoutCallback timeoutCallback[T],
	clearCallback clearCallback[T]) *TimeoutQueue[T] {
	timeoutQueue := &TimeoutQueue[T]{
		ctx:                 ctx,
		dp:                  newDelayQueue[T](),
		tickTime:            tickTime,
		timeoutCallBackFunc: timeoutCallback,
		clearCallBackFunc:   clearCallback,
	}

	return timeoutQueue
}

func (tq *TimeoutQueue[T]) Init() {
	if tq.timeoutCallBackFunc != nil {
		_ = S.Subscribe(queueItemTimeoutTopicName, tq.timeoutCallBackFunc)
	}

	go tq.checkTimeout()
}

func (tq *TimeoutQueue[T]) checkTimeout() {
	var tick = time.NewTicker(tq.tickTime)
	defer tick.Stop()
	for range tick.C {
		select {
		case <-tq.ctx.Done():
			return
		default:
		}
		for {
			now := time.Now()
			item := tq.dp.DelayPop(&now)
			if item == nil {
				break
			}

			if tq.timeoutCallBackFunc != nil {
				_ = S.GetPublisher(tq.ctx, queueItemTimeoutTopicName).Publish(tq.ctx, item.data, &now, item.delay)
			}
		}
	}
}

func (tq *TimeoutQueue[T]) Push(data T, expire *time.Time) {
	tq.dp.Push(expire, data)
}

func (tq *TimeoutQueue[T]) Clear() {
	now := time.Now()
	tq.dp.Clear(func(item *queueItem[T]) {
		if now.After(*item.delay) {
			if tq.timeoutCallBackFunc != nil {
				_ = S.GetPublisher(tq.ctx, queueItemTimeoutTopicName).Publish(tq.ctx, item.data, &now, item.delay)
			}
			return
		}

		if tq.clearCallBackFunc != nil {
			tq.clearCallBackFunc(tq.ctx, item.data)
		}
	})
}
