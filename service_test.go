package micro

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"go-micro.dev/v4/client"
	"go-micro.dev/v4/debug/handler"
	proto "go-micro.dev/v4/debug/proto"
	"go-micro.dev/v4/registry"
	"go-micro.dev/v4/server"
	"go-micro.dev/v4/transport"
	"go-micro.dev/v4/util/test"
)

func testShutdown(wg *sync.WaitGroup, cancel func()) {
	// add 1
	wg.Add(1)
	// shutdown the service
	cancel()
	// wait for stop
	wg.Wait()
}

func testService(t testing.TB, ctx context.Context, wg *sync.WaitGroup, name string) Service {
	// add self
	wg.Add(1)

	r := registry.NewMemoryRegistry(registry.Services(test.Data))

	// create service
	srv := NewService(
		Name(name),
		Context(ctx),
		Registry(r),
		AfterStart(func() error {
			wg.Done()

			return nil
		}),
		AfterStop(func() error {
			wg.Done()

			return nil
		}),
	)

	if err := RegisterHandler(srv.Server(), handler.NewHandler(srv.Client())); err != nil {
		t.Fatal(err)
	}

	return srv
}

func testCustomListenService(ctx context.Context, customListener net.Listener, wg *sync.WaitGroup, name string) Service {
	// add self
	wg.Add(1)

	r := registry.NewMemoryRegistry(registry.Services(test.Data))

	// create service
	srv := NewService(
		Name(name),
		Context(ctx),
		Registry(r),
		// injection customListener
		AddListenOption(server.ListenOption(transport.NetListener(customListener))),
		AfterStart(func() error {
			wg.Done()
			return nil
		}),
		AfterStop(func() error {
			wg.Done()
			return nil
		}),
	)

	RegisterHandler(srv.Server(), handler.NewHandler(srv.Client()))

	return srv
}

func testRequest(ctx context.Context, c client.Client, name string) error {
	// test call debug
	req := c.NewRequest(
		name,
		"Debug.Health",
		new(proto.HealthRequest),
	)

	rsp := new(proto.HealthResponse)

	err := c.Call(context.TODO(), req, rsp)
	if err != nil {
		return err
	}

	if rsp.Status != "ok" {
		return errors.New("service response: " + rsp.Status)
	}

	return nil
}

// TestService tests running and calling a service.
func TestService(t *testing.T) {
	// waitgroup for server start
	var wg sync.WaitGroup

	// cancellation context
	ctx, cancel := context.WithCancel(context.Background())

	// start test server
	service := testService(t, ctx, &wg, "test.service")

	go func() {
		// wait for service to start
		wg.Wait()

		// make a test call
		if err := testRequest(ctx, service.Client(), "test.service"); err != nil {
			t.Fatal(err)
		}

		// shutdown the service
		testShutdown(&wg, cancel)
	}()

	// start service
	if err := service.Run(); err != nil {
		t.Fatal(err)
	}
}

func benchmarkCustomListenService(b *testing.B, n int, name string) {
	// create custom listen
	customListen, err := net.Listen("tcp", server.DefaultAddress)
	if err != nil {
		b.Fatal(err)
	}

	// stop the timer
	b.StopTimer()

	// waitgroup for server start
	var wg sync.WaitGroup

	// cancellation context
	ctx, cancel := context.WithCancel(context.Background())

	// create test server
	service := testCustomListenService(ctx, customListen, &wg, name)

	// start the server
	go func() {
		if err := service.Run(); err != nil {
			b.Fatal(err)
		}
	}()

	// wait for service to start
	wg.Wait()

	// make a test call to warm the cache
	for i := 0; i < 10; i++ {
		if err := testRequest(ctx, service.Client(), name); err != nil {
			b.Fatal(err)
		}
	}

	// start the timer
	b.StartTimer()

	// number of iterations
	for i := 0; i < b.N; i++ {
		// for concurrency
		for j := 0; j < n; j++ {
			wg.Add(1)

			go func() {
				err := testRequest(ctx, service.Client(), name)
				wg.Done()
				if err != nil {
					b.Fatal(err)
				}
			}()
		}

		// wait for test completion
		wg.Wait()
	}

	// stop the timer
	b.StopTimer()

	// shutdown service
	testShutdown(&wg, cancel)
}

func benchmarkService(b *testing.B, n int, name string) {
	// stop the timer
	b.StopTimer()

	// waitgroup for server start
	var wg sync.WaitGroup

	// cancellation context
	ctx, cancel := context.WithCancel(context.Background())

	// create test server
	service := testService(b, ctx, &wg, name)

	// start the server
	go func() {
		if err := service.Run(); err != nil {
			b.Fatal(err)
		}
	}()

	// wait for service to start
	wg.Wait()

	// make a test call to warm the cache
	for i := 0; i < 10; i++ {
		if err := testRequest(ctx, service.Client(), name); err != nil {
			b.Fatal(err)
		}
	}

	// start the timer
	b.StartTimer()

	// number of iterations
	for i := 0; i < b.N; i++ {
		// for concurrency
		for j := 0; j < n; j++ {
			wg.Add(1)

			go func() {
				err := testRequest(ctx, service.Client(), name)

				wg.Done()

				if err != nil {
					b.Fatal(err)
				}
			}()
		}

		// wait for test completion
		wg.Wait()
	}

	// stop the timer
	b.StopTimer()

	// shutdown service
	testShutdown(&wg, cancel)
}

func BenchmarkService1(b *testing.B) {
	benchmarkService(b, 1, "test.service.1")
}

func BenchmarkService8(b *testing.B) {
	benchmarkService(b, 8, "test.service.8")
}

func BenchmarkService16(b *testing.B) {
	benchmarkService(b, 16, "test.service.16")
}

func BenchmarkService32(b *testing.B) {
	benchmarkService(b, 32, "test.service.32")
}

func BenchmarkService64(b *testing.B) {
	benchmarkService(b, 64, "test.service.64")
}

func BenchmarkCustomListenService1(b *testing.B) {
	benchmarkCustomListenService(b, 1, "test.service.1")
}
