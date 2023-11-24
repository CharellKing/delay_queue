package delay_queue

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

func TestSimpleScheduler(t *testing.T) {
	S.Subscribe("simple_test", func(value string) {
		if value != "helloworld" {
			t.Error(value)
		}
	})

	S.GetPublisher(context.Background(), "simple_test").Publish("helloworld")
	time.Sleep(5 * time.Millisecond)
}

func TestSimpleDelay(t *testing.T) {
	S.Subscribe("simple_test", func(value string) {
		t.Log(value)
	})

	S.GetPublisher(context.Background(), "simple_test").DelayWithMillisecond(2).Publish("helloworld")
	time.Sleep(3 * time.Millisecond)
}

func TestSchedulerWithParition(t *testing.T) {
	S.Subscribe("simple_test_with_partition", func(name string, i int) {
		time.Sleep(time.Duration(rand.Intn(10)) * time.Nanosecond)
		t.Logf("%s %d\n", name, i)
	})

	for i := 0; i < 10; i++ {
		S.GetPublisher(context.Background(), "simple_test_with_partition").Partition("partition1").Publish("partition1", i)
		S.GetPublisher(context.Background(), "simple_test_with_partition").Partition("partition2").Publish("partition2", i)
	}

	for i := 0; i < 10; i++ {
		S.GetPublisher(context.Background(), "simple_test_with_partition").Partition("partition3").Publish("partition3", i)
	}

	time.Sleep(5 * time.Millisecond)
}
