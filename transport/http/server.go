package http

import (
	"context"
	"fmt"
	"github.com/sreekar2307/queue/controller"
	"net/http"

	"github.com/sreekar2307/queue/config"
)

type Http struct {
	queue *controller.Queue

	server *http.Server
	config config.HTTP
}

func NewTransport(
	_ context.Context,
	config config.HTTP,
	queue *controller.Queue,
) (*Http, error) {
	transport := &Http{
		queue:  queue,
		config: config,
	}
	serverMux := http.NewServeMux()
	serverMux.HandleFunc("POST /topics", transport.createTopic)
	serverMux.HandleFunc("POST /connect", transport.connect)
	serverMux.HandleFunc("POST /sendMessage", transport.sendMessage)
	serverMux.HandleFunc("GET /receiveMessage", transport.receiveMessage)
	serverMux.HandleFunc("POST /ackMessage", transport.ackMessage)
	serverMux.HandleFunc("POST /healthCheck", transport.healthCheck)
	serverMux.HandleFunc("GET /shards-info", transport.shardsInfo)
	server := http.Server{
		Addr:    config.ListenerAddr,
		Handler: serverMux,
	}
	transport.server = &server
	return transport, nil
}

func (h *Http) Start(_ context.Context) error {
	return h.server.ListenAndServe()
}

func (h *Http) Close(ctx context.Context) error {
	if err := h.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown server: %w", err)
	}
	return h.queue.Close(ctx)
}
