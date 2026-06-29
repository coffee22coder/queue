package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func QueueHandler(broker *Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		queue := strings.Trim(r.URL.Path, "/")
		if queue == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodPut:
			v := r.URL.Query().Get("v")
			if v == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			broker.Put(queue, v)
			w.WriteHeader(http.StatusOK)
			return
		case http.MethodGet:
			var val string
			var ok bool
			timeoutStr := r.URL.Query().Get("timeout")
			if timeoutStr != "" {
				timeoutInt, err := strconv.Atoi(timeoutStr)
				if err != nil || timeoutInt < 0 {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				timeout := time.Duration(timeoutInt) * time.Second
				val, ok = broker.GetWait(r.Context(), queue, timeout)
			} else {
				val, ok = broker.Get(queue)
			}
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(val))
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	}
}

type waiter struct {
	ch chan string
}

type queueState struct {
	messages []string
	waiters  []*waiter
}

type Broker struct {
	mu     sync.Mutex
	queues map[string]*queueState
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*queueState),
	}
}

// Вызывать только под Lock
func (b *Broker) getQueueLocked(name string) *queueState {
	q := b.queues[name]
	if q == nil {
		q = &queueState{}
		b.queues[name] = q
	}
	return q
}

// Вызывать только под Lock
func (b *Broker) removeWaiterLocked(q *queueState, target *waiter) {
	for i, w := range q.waiters {
		if w == target {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return
		}
	}
}

func (b *Broker) Put(queue, value string) {
	b.mu.Lock()
	var w *waiter

	q := b.getQueueLocked(queue)
	if len(q.waiters) > 0 {
		w = q.waiters[0]
		q.waiters = q.waiters[1:]
	} else {
		q.messages = append(q.messages, value)
	}

	b.mu.Unlock()

	if w != nil {
		w.ch <- value
	}
}

func (b *Broker) Get(queue string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.getQueueLocked(queue)
	if len(q.messages) == 0 {
		return "", false
	}
	val := q.messages[0]
	q.messages = q.messages[1:]
	return val, true
}

func (b *Broker) GetWait(ctx context.Context, queue string, timeout time.Duration) (string, bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	b.mu.Lock()
	q := b.getQueueLocked(queue)
	if len(q.messages) > 0 {
		val := q.messages[0]
		q.messages = q.messages[1:]
		b.mu.Unlock()
		return val, true
	}
	if timeout <= 0 {
		b.mu.Unlock()
		return "", false
	}
	w := &waiter{ch: make(chan string, 1)}
	q.waiters = append(q.waiters, w)
	b.mu.Unlock()

	select {
	case <-ctx.Done():
		select {
		case msg := <-w.ch:
			return msg, true
		default:
			b.mu.Lock()
			q := b.getQueueLocked(queue)
			b.removeWaiterLocked(q, w)
			b.mu.Unlock()
		}
		return "", false
	case msg := <-w.ch:
		return msg, true
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: queue-broker <port>")
		os.Exit(1)
	}

	port := os.Args[1]
	if _, err := strconv.Atoi(port); err != nil {
		fmt.Fprintln(os.Stderr, "invalid port")
		os.Exit(1)
	}
	addr := ":" + port

	broker := NewBroker()

	mux := http.NewServeMux()
	mux.HandleFunc("/", QueueHandler(broker))

	server := http.Server{
		Addr:    addr,
		Handler: mux,
	}

	fmt.Println("starting server at :8080")
	server.ListenAndServe()
}
