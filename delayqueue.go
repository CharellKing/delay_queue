package delay_queue

import (
	"container/heap"
	"sync"
	"time"
)

type queueItem[T any] struct {
	data  T
	delay *time.Time
}

type queueItems[T any] []*queueItem[T]

func (dq queueItems[T]) Len() int {
	return len(dq)
}

func (dq queueItems[T]) Less(i, j int) bool {
	return dq[i].delay.Before(*dq[j].delay)
}

func (dq queueItems[T]) Swap(i, j int) {
	dq[i], dq[j] = dq[j], dq[i]
}

func (dq *queueItems[T]) Push(x any) {
	item := x.(*queueItem[T])
	*dq = append(*dq, item)
}

func (dq *queueItems[T]) Pop() any {
	old := *dq
	n := len(old)
	item := old[n-1]
	*dq = old[:n-1]
	return item
}

type clearCallBack[T any] func(item *queueItem[T])

type DelayQueue[T any] struct {
	items queueItems[T]
	lock  sync.RWMutex
}

func newDelayQueue[T any]() *DelayQueue[T] {
	return &DelayQueue[T]{
		items: make(queueItems[T], 0),
	}
}

func (q *DelayQueue[T]) Push(delay *time.Time, data T) {
	q.lock.Lock()
	defer q.lock.Unlock()
	heap.Push(&q.items, &queueItem[T]{
		delay: delay,
		data:  data,
	})
}

func (q *DelayQueue[T]) DelayPop(value *time.Time) *queueItem[T] {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.items) <= 0 {
		return nil
	}
	if q.items[0].delay.After(*value) {
		return nil
	}
	return heap.Pop(&q.items).(*queueItem[T])
}

func (q *DelayQueue[T]) IsEmpty() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.items) == 0
}

func (q *DelayQueue[T]) Pop() *queueItem[T] {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.items) <= 0 {
		return nil
	}
	return heap.Pop(&q.items).(*queueItem[T])
}

func (q *DelayQueue[T]) Clear(callback clearCallBack[T]) {
	q.lock.Lock()
	var oldQueueItems = q.items
	q.items = make(queueItems[T], 0)
	q.lock.Unlock()

	for _, item := range oldQueueItems {
		callback(item)
	}
}
