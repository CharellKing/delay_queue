package delay_queue

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"
)

type schedulerTopic struct {
	topic      string
	partitions sync.Map
	callback   reflect.Value
}

func newSchedulerTopic(topic string, fn interface{}) (*schedulerTopic, error) {
	if !(reflect.TypeOf(fn).Kind() == reflect.Func) {
		return nil, fmt.Errorf("%s is not of type reflect.Func", reflect.TypeOf(fn).Kind())
	}

	t := &schedulerTopic{
		topic:    topic,
		callback: reflect.ValueOf(fn),
	}
	return t, nil
}

func (t *schedulerTopic) push(ctx context.Context, partitionName string, delay *time.Time, args []interface{}) {
	partitionValue, loaded := t.partitions.LoadOrStore(partitionName, newSchedulerPartition(partitionName, t.callback))
	partition := partitionValue.(*schedulerPartition)
	partition.push(delay, args)
	if !loaded {
		go partition.run(ctx)
	}
}
