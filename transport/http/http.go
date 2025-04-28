package http

import (
	"context"
	"fmt"
	"net/http"
	"queue/model"
	"queue/service"
	"time"
)

type Http struct {
	queue *service.Queue

	server *http.Server
}

func NewTransport(
	ctx context.Context,
	config service.Config,
) (*Http, error) {
	queue, err := service.NewQueue(
		ctx,
		config,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %w", err)
	}
	transport := &Http{
		queue: queue,
	}
	serverMux := http.NewServeMux()
	serverMux.HandleFunc("POST /topics", transport.createTopic)
	serverMux.HandleFunc("POST /connect", transport.connect)
	serverMux.HandleFunc("POST /sendMessage", transport.sendMessage)
	serverMux.HandleFunc("GET /receiveMessage", transport.receiveMessage)
	serverMux.HandleFunc("POST /ackMessage", transport.ackMessage)
	serverMux.HandleFunc("POST /healthCheck", transport.healthCheck)
	server := http.Server{
		Addr:    ":8000",
		Handler: serverMux,
	}
	transport.server = &server
	return transport, nil
}

func (h *Http) Start(context.Context) error {
	return h.server.ListenAndServe()
}

func (h *Http) Connect(
	ctx context.Context,
	consumerID string,
	consumerGroup string,
	topics []string,
) (*model.Consumer, *model.ConsumerGroup, error) {
	consumer, group, err := h.queue.Connect(ctx, consumerID, consumerGroup, topics)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect: %w", err)
	}
	return consumer, group, nil
}

func (h *Http) Close(ctx context.Context) error {
	if err := h.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown server: %w", err)
	}
	return h.queue.Close(ctx)
}

func (h *Http) CreateTopic(rCtx context.Context, name string, numberOfPartitions uint64) (*model.Topic, error) {
	return h.queue.CreateTopic(rCtx, name, numberOfPartitions)
}

func (h *Http) SendMessage(rCtx context.Context, msg *model.Message) (*model.Message, error) {
	return h.queue.SendMessage(rCtx, msg)
}

func (h *Http) ReceiveMessage(ctx context.Context, consumerID string) (*model.Message, error) {
	return h.queue.ReceiveMessage(ctx, consumerID)
}

func (h *Http) AckMessage(ctx context.Context, consumerID string, message *model.Message) error {
	return h.queue.AckMessage(ctx, consumerID, message)
}

func (h *Http) HealthCheck(ctx context.Context, consumerID string, pingAt time.Time) (*model.Consumer, error) {
	return h.queue.HealthCheck(ctx, consumerID, pingAt)
}
