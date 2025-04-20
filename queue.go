package queue

// type EmbeddedTransport struct {
// 	topics map[string]*Topic
//
// 	mu sync.Mutex
// }
//
// func NewEmbededTransport() Transport {
// 	return &EmbeddedTransport{
// 		topics: make(map[string]*Topic),
// 	}
// }
//
// func (q *EmbeddedTransport) Close(context.Context) error {
// 	return nil
// }
//
// func (q *EmbeddedTransport) Connect(context.Context, string, string) error {
// 	return nil
// }
//
// func (q *EmbeddedTransport) CreateTopic(ctx context.Context, name string) (*Topic, error) {
// 	if _, ok := q.topics[name]; ok {
// 		return nil, fmt.Errorf("topic '%s' already exists", name)
// 	}
// 	q.mu.Lock()
// 	defer q.mu.Unlock()
// 	// q.topics[name] = NewTopic(ctx, name, selection.NewRoundRobinPartitionSelectionStrategy())
// 	return q.topics[name], nil
// }
//
// func (q *EmbeddedTransport) SendMessage(
// 	ctx context.Context,
// 	topic string,
// 	msg *Message,
// ) (*Message, error) {
// 	return nil, nil
// }
//
// func (q *EmbeddedTransport) SendMessageToPartition(
// 	ctx context.Context,
// 	topic, partition string,
// 	msg *Message,
// ) (*Message, error) {
// }
//
// func (q *EmbeddedTransport) ReceiveMessage(
// 	ctx context.Context,
// 	topic, consumerGroup string,
// ) (*Message, error) {
// 	if _, ok := q.topics[topic]; !ok {
// 		return nil, fmt.Errorf("topic '%s' does not exist", topic)
// 	}
// 	return q.topics[topic].ReceiveMessage(ctx, consumerGroup)
// }
//
// func (q *EmbeddedTransport) AckMessage(
// 	ctx context.Context,
// 	topic, consumerGroup string,
// 	message *Message,
// ) error {
// 	if _, ok := q.topics[topic]; !ok {
// 		return fmt.Errorf("topic '%s' does not exist", topic)
// 	}
// 	return q.topics[topic].AckMessage(ctx, message, consumerGroup)
// }
