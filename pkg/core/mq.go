package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// QueueInfo 队列信息主结构
type QueueInfo struct {
	ConsumerDetails               []ConsumerDetail  `json:"consumer_details"`
	Arguments                     map[string]any    `json:"arguments"`
	AutoDelete                    bool              `json:"auto_delete"`
	ConsumerCapacity              float64           `json:"consumer_capacity"`
	ConsumerUtilisation           float64           `json:"consumer_utilisation"`
	Consumers                     int               `json:"consumers"`
	Deliveries                    []any             `json:"deliveries"`
	Durable                       bool              `json:"durable"`
	EffectivePolicyDefinition     map[string]any    `json:"effective_policy_definition"`
	Exclusive                     bool              `json:"exclusive"`
	ExclusiveConsumerTag          any               `json:"exclusive_consumer_tag"`
	GarbageCollection             GarbageCollection `json:"garbage_collection"`
	HeadMessageTimestamp          any               `json:"head_message_timestamp"`
	Incoming                      []any             `json:"incoming"`
	Memory                        int64             `json:"memory"`
	MessageBytes                  int64             `json:"message_bytes"`
	MessageBytesPagedOut          int64             `json:"message_bytes_paged_out"`
	MessageBytesPersistent        int64             `json:"message_bytes_persistent"`
	MessageBytesRam               int64             `json:"message_bytes_ram"`
	MessageBytesReady             int64             `json:"message_bytes_ready"`
	MessageBytesUnacknowledged    int64             `json:"message_bytes_unacknowledged"`
	MessageStats                  MessageStats      `json:"message_stats"`
	Messages                      int               `json:"messages"`
	MessagesDetails               RateDetail        `json:"messages_details"`
	MessagesPagedOut              int               `json:"messages_paged_out"`
	MessagesPersistent            int               `json:"messages_persistent"`
	MessagesRam                   int               `json:"messages_ram"`
	MessagesReady                 int               `json:"messages_ready"`
	MessagesReadyDetails          RateDetail        `json:"messages_ready_details"`
	MessagesReadyRam              int               `json:"messages_ready_ram"`
	MessagesUnacknowledged        int               `json:"messages_unacknowledged"`
	MessagesUnacknowledgedDetails RateDetail        `json:"messages_unacknowledged_details"`
	MessagesUnacknowledgedRam     int               `json:"messages_unacknowledged_ram"`
	Name                          string            `json:"name"`
	Node                          string            `json:"node"`
	OperatorPolicy                any               `json:"operator_policy"`
	Policy                        any               `json:"policy"`
	RecoverableSlaves             any               `json:"recoverable_slaves"`
	Reductions                    int64             `json:"reductions"`
	ReductionsDetails             RateDetail        `json:"reductions_details"`
	SingleActiveConsumerTag       any               `json:"single_active_consumer_tag"`
	State                         string            `json:"state"`
	Type                          string            `json:"type"`
	Vhost                         string            `json:"vhost"`
}

// ConsumerDetail 消费者详细信息
type ConsumerDetail struct {
	Arguments       map[string]any `json:"arguments"`
	ChannelDetails  ChannelDetail  `json:"channel_details"`
	AckRequired     bool           `json:"ack_required"`
	Active          bool           `json:"active"`
	ActivityStatus  string         `json:"activity_status"`
	ConsumerTag     string         `json:"consumer_tag"`
	ConsumerTimeout int            `json:"consumer_timeout"`
	Exclusive       bool           `json:"exclusive"`
	PrefetchCount   int            `json:"prefetch_count"`
	Queue           QueueReference `json:"queue"`
}

// ChannelDetail 通道详细信息
type ChannelDetail struct {
	ConnectionName string `json:"connection_name"`
	Name           string `json:"name"`
	Node           string `json:"node"`
	Number         int    `json:"number"`
	PeerHost       string `json:"peer_host"`
	PeerPort       int    `json:"peer_port"`
	User           string `json:"user"`
}

// QueueReference 队列引用
type QueueReference struct {
	Name  string `json:"name"`
	Vhost string `json:"vhost"`
}

// GarbageCollection GC信息
type GarbageCollection struct {
	FullsweepAfter  int `json:"fullsweep_after"`
	MaxHeapSize     int `json:"max_heap_size"`
	MinBinVheapSize int `json:"min_bin_vheap_size"`
	MinHeapSize     int `json:"min_heap_size"`
	MinorGCs        int `json:"minor_gcs"`
}

// MessageStats 消息统计信息
type MessageStats struct {
	Ack                 int64      `json:"ack"`
	AckDetails          RateDetail `json:"ack_details"`
	Deliver             int64      `json:"deliver"`
	DeliverDetails      RateDetail `json:"deliver_details"`
	DeliverGet          int64      `json:"deliver_get"`
	DeliverGetDetails   RateDetail `json:"deliver_get_details"`
	DeliverNoAck        int64      `json:"deliver_no_ack"`
	DeliverNoAckDetails RateDetail `json:"deliver_no_ack_details"`
	Get                 int64      `json:"get"`
	GetDetails          RateDetail `json:"get_details"`
	GetEmpty            int64      `json:"get_empty"`
	GetEmptyDetails     RateDetail `json:"get_empty_details"`
	GetNoAck            int64      `json:"get_no_ack"`
	GetNoAckDetails     RateDetail `json:"get_no_ack_details"`
	Publish             int64      `json:"publish"`
	PublishDetails      RateDetail `json:"publish_details"`
	Redeliver           int64      `json:"redeliver"`
	RedeliverDetails    RateDetail `json:"redeliver_details"`
}

// RateDetail 速率详情
type RateDetail struct {
	Rate float64 `json:"rate"`
}

// RabbitMQConfig RabbitMQ配置
type RabbitMQConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Vhost    string
}

// RabbitMQClient RabbitMQ客户端
type RabbitMQClient struct {
	baseURL    string
	username   string
	password   string
	httpClient *http.Client
	mu         sync.RWMutex // 保护并发访问
}

var (
	// 全局单例客户端实例
	defaultClient *RabbitMQClient
	// 存储不同配置的客户端实例
	clients   = make(map[string]*RabbitMQClient)
	clientsMu sync.RWMutex
)

// NewRabbitMQClient 创建新的RabbitMQ客户端
func NewRabbitMQClient(config *RabbitMQConfig) *RabbitMQClient {
	// 配置HTTP传输层，启用连接池
	transport := &http.Transport{
		MaxIdleConns:        100,              // 最大空闲连接数
		MaxIdleConnsPerHost: 10,               // 每个主机的最大空闲连接数
		IdleConnTimeout:     90 * time.Second, // 空闲连接超时时间
		DisableCompression:  false,            // 启用压缩
		DisableKeepAlives:   false,            // 启用Keep-Alive
	}

	return &RabbitMQClient{
		baseURL:  fmt.Sprintf("http://%s:%d/api", config.Host, config.Port),
		username: config.Username,
		password: config.Password,
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
	}
}

// GetDefaultClient 获取默认客户端（使用默认配置）
func GetDefaultClient() *RabbitMQClient {
	if defaultClient == nil {
		panic("默认客户端未初始化，请先调用 InitDefaultClient")
	}
	return defaultClient
}

// InitDefaultClient 初始化默认客户端
func InitDefaultClient(config *RabbitMQConfig) {
	defaultClient = NewRabbitMQClient(config)
}

// GetOrCreateClient 获取或创建客户端（基于配置的缓存）
func GetOrCreateClient(config *RabbitMQConfig) *RabbitMQClient {
	// 生成配置的唯一键
	key := fmt.Sprintf("%s:%d:%s", config.Host, config.Port, config.Username)

	clientsMu.RLock()
	if client, exists := clients[key]; exists {
		clientsMu.RUnlock()
		return client
	}
	clientsMu.RUnlock()

	// 创建新客户端
	clientsMu.Lock()
	defer clientsMu.Unlock()

	// 双重检查，防止重复创建
	if client, exists := clients[key]; exists {
		return client
	}

	client := NewRabbitMQClient(config)
	clients[key] = client
	return client
}

// ClearClients 清理所有客户端（用于测试或重新加载配置）
func ClearClients() {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	for _, client := range clients {
		client.httpClient.CloseIdleConnections()
	}
	clients = make(map[string]*RabbitMQClient)

	if defaultClient != nil {
		defaultClient.httpClient.CloseIdleConnections()
		defaultClient = nil
	}
}

// encodeVhost 编码vhost名称
func encodeVhost(vhost string) string {
	if vhost == "/" {
		return "%2F"
	}
	// 将斜杠替换为 %2F
	return url.PathEscape(vhost)
}

// GetQueueInfo 获取指定vhost和队列名的队列信息
func (c *RabbitMQClient) GetQueueInfo(vhost, queueName string) (*QueueInfo, error) {
	// 编码vhost名称
	encodedVhost := encodeVhost(vhost)
	// 队列名不需要额外编码，直接使用
	encodedQueueName := queueName

	url := fmt.Sprintf("%s/queues/%s/%s", c.baseURL, encodedVhost, encodedQueueName)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}

	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API返回错误状态码: %d", resp.StatusCode)
	}

	var queueInfo QueueInfo
	if err := json.NewDecoder(resp.Body).Decode(&queueInfo); err != nil {
		return nil, fmt.Errorf("解析JSON失败: %w", err)
	}

	return &queueInfo, nil
}

// GetQueueInfoWithVhost 使用客户端配置的默认vhost获取队列信息
func (c *RabbitMQClient) GetQueueInfoWithDefaultVhost(queueName string) (*QueueInfo, error) {
	// 这里需要从配置中获取默认vhost，但当前客户端没有存储配置
	// 建议在RabbitMQClient中添加config字段
	return nil, fmt.Errorf("需要实现默认vhost功能")
}

// GetAllQueues 获取所有队列信息
func (c *RabbitMQClient) GetAllQueues(vhost string) ([]QueueInfo, error) {
	encodedVhost := encodeVhost(vhost)
	url := fmt.Sprintf("%s/queues/%s", c.baseURL, encodedVhost)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}

	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API返回错误状态码: %d", resp.StatusCode)
	}

	var queues []QueueInfo
	if err := json.NewDecoder(resp.Body).Decode(&queues); err != nil {
		return nil, fmt.Errorf("解析JSON失败: %w", err)
	}

	return queues, nil
}

// GetQueueStats 获取队列统计信息（简化版）
func (c *RabbitMQClient) GetQueueStats(vhost, queueName string) (*QueueStats, error) {
	info, err := c.GetQueueInfo(vhost, queueName)
	if err != nil {
		return nil, err
	}

	return &QueueStats{
		Name:                   info.Name,
		Vhost:                  info.Vhost,
		Messages:               info.Messages,
		MessagesReady:          info.MessagesReady,
		MessagesUnacknowledged: info.MessagesUnacknowledged,
		Consumers:              info.Consumers,
		ConsumerUtilisation:    info.ConsumerUtilisation,
		MessageBytes:           info.MessageBytes,
		Memory:                 info.Memory,
		State:                  info.State,
		PublishRate:            info.MessageStats.PublishDetails.Rate,
		DeliverRate:            info.MessageStats.DeliverDetails.Rate,
		AckRate:                info.MessageStats.AckDetails.Rate,
		ReductionsRate:         info.ReductionsDetails.Rate,
	}, nil
}

// QueueStats 队列统计信息（简化版）
type QueueStats struct {
	Name                   string
	Vhost                  string
	Messages               int
	MessagesReady          int
	MessagesUnacknowledged int
	Consumers              int
	ConsumerUtilisation    float64
	MessageBytes           int64
	Memory                 int64
	State                  string
	PublishRate            float64
	DeliverRate            float64
	AckRate                float64
	ReductionsRate         float64
}

// IsHealthy 检查队列是否健康
func (q *QueueInfo) IsHealthy() bool {
	return q.State == "running" && q.Consumers > 0
}

// GetConsumerCount 获取消费者数量
func (q *QueueInfo) GetConsumerCount() int {
	return q.Consumers
}

// GetReadyMessageCount 获取待消费消息数
func (q *QueueInfo) GetReadyMessageCount() int {
	return q.MessagesReady
}

// GetUnacknowledgedMessageCount 获取未确认消息数
func (q *QueueInfo) GetUnacknowledgedMessageCount() int {
	return q.MessagesUnacknowledged
}

// GetTotalMessageCount 获取总消息数
func (q *QueueInfo) GetTotalMessageCount() int {
	return q.Messages
}

// GetMessageRate 获取消息速率（每秒）
func (q *QueueInfo) GetMessageRate() float64 {
	return q.MessagesDetails.Rate
}

// String 实现Stringer接口，便于打印
func (q *QueueInfo) String() string {
	return fmt.Sprintf("Queue: %s, Vhost: %s, State: %s, Messages: %d, Consumers: %d, Ready: %d, Unacked: %d",
		q.Name, q.Vhost, q.State, q.Messages, q.Consumers, q.MessagesReady, q.MessagesUnacknowledged)
}

// Close 关闭客户端，释放资源
func (c *RabbitMQClient) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}
