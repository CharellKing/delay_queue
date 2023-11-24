package delay_queue

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"sync"
	"time"
)

type scheduler struct {
	topics sync.Map
}

var S scheduler

func (s *scheduler) Subscribe(topic string, fn interface{}) error {
	schedulerTopic, err := newSchedulerTopic(topic, fn)
	if err != nil {
		return errors.WithStack(err)
	}

	if _, loaded := s.topics.LoadOrStore(topic, schedulerTopic); loaded {
		return fmt.Errorf("topic %s already subscribed", topic)
	}

	return nil
}

func (t *scheduler) GetPublisher(ctx context.Context, topicName string) *schedulerPublisher {
	var (
		topic *schedulerTopic
		err   error
	)

	topicVal, ok := S.topics.Load(topicName)
	if ok {
		topic = topicVal.(*schedulerTopic)
	} else {
		err = fmt.Errorf("%s topic not exists", topicName)
	}

	return &schedulerPublisher{
		ctx:           ctx,
		topic:         topic,
		delay:         lo.ToPtr(time.Now()),
		partitionName: "default",
		err:           err,
	}
}

type schedulerPublisher struct {
	ctx           context.Context
	topic         *schedulerTopic
	delay         *time.Time
	partitionName string

	err error
}

func (publisher *schedulerPublisher) DelayWithSecond(delay int64) *schedulerPublisher {
	publisher.delay = lo.ToPtr(time.Now().Add(time.Second * time.Duration(delay)))
	return publisher
}

func (publisher *schedulerPublisher) DelayWithMillisecond(delay int64) *schedulerPublisher {
	publisher.delay = lo.ToPtr(time.Now().Add(time.Millisecond * time.Duration(delay)))
	return publisher
}

func (publisher *schedulerPublisher) DelayWithMinute(delay int64) *schedulerPublisher {
	publisher.delay = lo.ToPtr(time.Now().Add(time.Minute * time.Duration(delay)))
	return publisher
}

func (publisher *schedulerPublisher) Partition(partition string) *schedulerPublisher {
	publisher.partitionName = partition
	return publisher
}

func (publisher *schedulerPublisher) Publish(args ...interface{}) error {
	if publisher.err != nil {
		return errors.WithStack(publisher.err)
	}

	publisher.topic.push(publisher.ctx, publisher.partitionName, publisher.delay, args)
	return nil
}
