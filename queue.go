package main

import (
	"context"
	"github.com/sreekar2307/queue/logger/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"log"
	"os/signal"
	"syscall"

	"github.com/sreekar2307/queue/config"
	"github.com/sreekar2307/queue/controller"
	"github.com/sreekar2307/queue/transport"
	"github.com/sreekar2307/queue/transport/grpc"
	"github.com/sreekar2307/queue/transport/http"
)

func main() {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGKILL, syscall.SIGTERM)
	defer cancel()
	if config.Conf.ShowVersion {
		config.Conf.PrintVersion()
		return
	} else if config.Conf.Help {
		config.Conf.PrintUsage()
		return
	}

	tracer := otel.GetTracerProvider().Tracer(
		config.Conf.Scope(),
		trace.WithInstrumentationVersion(config.Conf.Version()),
	)

	logger := zerolog.NewLogger(config.Conf.Level)
	queue, err := controller.NewQueue(ctx, tracer, logger)
	if err != nil {
		log.Fatalf("failed to create queue: %v", err)
	}
	transporters := startTransporters(ctx, tracer, queue)
	<-ctx.Done()
	for _, transporter := range transporters {
		ctx, cancel = context.WithTimeout(context.Background(), config.Conf.ShutdownTimeout)
		if err := transporter.Close(ctx); err != nil {
			log.Printf("failed to close transporter: %v", err)
		}
		cancel()
	}
}

func startTransporters(ctx context.Context, tracer trace.Tracer, queue *controller.Queue) []transport.Transport {
	// Start the transporters
	transporters := make([]transport.Transport, 0)
	if config.Conf.GRPC.ListenerAddr != "" {
		grpcTransport, err := grpc.NewTransport(ctx, config.Conf.GRPC, queue)
		if err != nil {
			log.Fatalf("failed to create gRPC transport: %v", err)
		}
		go func() {
			if err := grpcTransport.Start(ctx); err != nil {
				log.Fatalf("failed to start gRPC transport: %v", err)
			}
		}()
		transporters = append(transporters, grpcTransport)
	}
	if config.Conf.HTTP.ListenerAddr != "" {

		httpTransport, err := http.NewTransport(ctx, config.Conf.HTTP, queue)
		if err != nil {
			log.Fatalf("failed to create HTTP transport: %v", err)
		}
		go func() {
			if err := httpTransport.Start(ctx); err != nil {
				log.Fatalf("failed to start HTTP transport: %v", err)
			}
		}()
		transporters = append(transporters, httpTransport)
	}
	return transporters
}
