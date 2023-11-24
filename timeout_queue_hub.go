package delay_queue

import (
	"context"
	"sync"
	"time"
)

type TimeoutQueueHub[T any] struct {
	queues sync.Map

	tickTime            time.Duration
	timeoutCallBackFunc timeoutCallback[T]
	clearCallBackFunc   clearCallback[T]
}

func NewTimeoutQueueHub[T any](opts ...func(hub *TimeoutQueueHub[T])) *TimeoutQueueHub[T] {
	hub := &TimeoutQueueHub[T]{
		queues:              sync.Map{},
		tickTime:            time.Millisecond,
		timeoutCallBackFunc: nil,
		clearCallBackFunc:   nil,
	}
	for _, opt := range opts {
		opt(hub)
	}
	return hub
}

func WithTickTime[T any](tickTime time.Duration) func(hub *TimeoutQueueHub[T]) {
	return func(hub *TimeoutQueueHub[T]) {
		hub.tickTime = tickTime
	}
}

func WithTimeoutCallback[T any](callback timeoutCallback[T]) func(hub *TimeoutQueueHub[T]) {
	return func(hub *TimeoutQueueHub[T]) {
		hub.timeoutCallBackFunc = callback
	}
}

func WithClearCallback[T any](callback clearCallback[T]) func(hub *TimeoutQueueHub[T]) {
	return func(hub *TimeoutQueueHub[T]) {
		hub.clearCallBackFunc = callback
	}
}

func (hub *TimeoutQueueHub[T]) GetQueue(ctx context.Context, queueName string) *TimeoutQueue[T] {
	timeoutQueueValue, loaded := hub.queues.LoadOrStore(queueName, newTimeoutQueue(
		ctx, hub.tickTime, hub.timeoutCallBackFunc, hub.clearCallBackFunc,
	))

	timeoutQueue := timeoutQueueValue.(*TimeoutQueue[T])
	if !loaded {
		timeoutQueue.Init()
	}
	return timeoutQueue
}
