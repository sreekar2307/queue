package http

import (
	"encoding/json"
	"net/http"
	"queue/model"
	"time"
)

type createTopicReqBody struct {
	Name               string `json:"name"`
	NumberOfPartitions uint64 `json:"numberOfPartitions"`
	ReplicationFactor  uint64 `json:"replicationFactor"`
}

func (h *Http) createTopic(w http.ResponseWriter, r *http.Request) {
	var (
		reqBody createTopicReqBody
		ctx     = r.Context()
	)
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if reqBody.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if reqBody.NumberOfPartitions <= 0 && reqBody.NumberOfPartitions > 10 {
		http.Error(w, "numberOfPartitions must be between 1 and 10", http.StatusBadRequest)
		return
	}
	if reqBody.ReplicationFactor <= 0 && reqBody.ReplicationFactor > 3 {
		http.Error(w, "replicationFactor must be between 1 and 3", http.StatusBadRequest)
		return
	}
	topic, err := h.CreateTopic(
		ctx,
		reqBody.Name,
		reqBody.NumberOfPartitions,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	responseBody, err := json.Marshal(topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(responseBody)
}

type connectReqBody struct {
	ConsumerID    string   `json:"consumerID"`
	ConsumerGroup string   `json:"consumerGroup"`
	Topics        []string `json:"topics"`
}

type connectRespBody struct {
	Consumer      *model.Consumer      `json:"consumer"`
	ConsumerGroup *model.ConsumerGroup `json:"consumerGroup"`
}

func (h *Http) connect(w http.ResponseWriter, r *http.Request) {
	var (
		ctx     = r.Context()
		reqBody connectReqBody
	)
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if reqBody.ConsumerID == "" {
		http.Error(w, "consumerID is required", http.StatusBadRequest)
		return
	}
	if reqBody.ConsumerGroup == "" {
		http.Error(w, "consumerGroup is required", http.StatusBadRequest)
		return
	}
	if len(reqBody.Topics) == 0 {
		http.Error(w, "topics is required", http.StatusBadRequest)
		return
	}
	consumerID := reqBody.ConsumerID
	consumer, consumerGroup, err := h.Connect(
		ctx,
		consumerID,
		reqBody.ConsumerGroup,
		reqBody.Topics,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	responseBody := connectRespBody{
		Consumer:      consumer,
		ConsumerGroup: consumerGroup,
	}
	response, err := json.Marshal(responseBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

type disconnectReqBody struct {
	ConsumerID    string   `json:"consumerID"`
	ConsumerGroup string   `json:"consumerGroup"`
	Topics        []string `json:"topics"`
}

func (h *Http) sendMessage(w http.ResponseWriter, r *http.Request) {
	var (
		ctx     = r.Context()
		reqBody model.Message
	)
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if reqBody.Topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}
	if len(reqBody.Data) == 0 {
		http.Error(w, "data is required", http.StatusBadRequest)
		return
	}
	message, err := h.SendMessage(
		ctx,
		&reqBody,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	responseBody, err := json.Marshal(message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseBody)
}

type receiveMessageReqBody struct {
	ConsumerID string `json:"consumerID"`
}

func (h *Http) receiveMessage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	defer r.Body.Close()
	var reqBody receiveMessageReqBody
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	message, err := h.ReceiveMessage(
		ctx,
		reqBody.ConsumerID,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	responseBody, err := json.Marshal(message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseBody)
}

type ackMessageReqBody struct {
	ConsumerID  string `json:"consumerID"`
	PartitionID string `json:"partitionID"`
	ID          []byte `json:"ID"`
}

func (h *Http) ackMessage(w http.ResponseWriter, r *http.Request) {
	var (
		ctx     = r.Context()
		reqBody ackMessageReqBody
	)
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(reqBody.ID) == 0 {
		http.Error(w, "message ID is required", http.StatusBadRequest)
		return
	}
	if reqBody.PartitionID == "" {
		http.Error(w, "partitionID is required", http.StatusBadRequest)
		return
	}
	err := h.AckMessage(
		ctx,
		reqBody.ConsumerID,
		&model.Message{ID: reqBody.ID, PartitionID: reqBody.PartitionID},
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type healthCheckReqBody struct {
	ConsumerID string    `json:"consumerID"`
	PingAt     time.Time `json:"pingAt"`
}

func (h *Http) healthCheck(w http.ResponseWriter, r *http.Request) {
	var (
		ctx     = r.Context()
		reqBody healthCheckReqBody
	)
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if reqBody.ConsumerID == "" {
		http.Error(w, "consumerID is required", http.StatusBadRequest)
		return
	}
	if reqBody.PingAt.IsZero() {
		http.Error(w, "pingAt is required", http.StatusBadRequest)
		return
	}
	consumer, err := h.HealthCheck(
		ctx,
		reqBody.ConsumerID,
		reqBody.PingAt,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	responseBody, err := json.Marshal(consumer)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseBody)
}
