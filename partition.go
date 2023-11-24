package delay_queue

import (
	"context"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"reflect"
	"time"
)

type schedulerPartition struct {
	partitionName string
	dq            *DelayQueue[[]interface{}]
	callback      reflect.Value
}

func newSchedulerPartition(partitionName string, callback reflect.Value) *schedulerPartition {
	t := &schedulerPartition{
		partitionName: partitionName,
		dq:            newDelayQueue[[]interface{}](),
		callback:      callback,
	}
	return t
}

func (t *schedulerPartition) push(delay *time.Time, args []interface{}) {
	t.dq.Push(delay, args)
}

func (t *schedulerPartition) run(ctx context.Context) {
	var tick = time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for range tick.C {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t.pop(ctx)
	}
}

func (t *schedulerPartition) call(item *queueItem[[]interface{}]) {
	funcType := t.callback.Type()
	args := cast.ToSlice(item.data)
	passedArguments := make([]reflect.Value, len(args))
	for i, v := range args {
		if v == nil {
			passedArguments[i] = reflect.New(funcType.In(i)).Elem()
		} else {
			passedArguments[i] = reflect.ValueOf(v)
		}
	}

	t.callback.Call(passedArguments)
}

func (t *schedulerPartition) pop(_ context.Context) {
	item := t.dq.DelayPop(lo.ToPtr(time.Now()))
	if item == nil {
		return
	}

	t.call(item)
}
