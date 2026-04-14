package core

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

func TestMq(t *testing.T) {
	// 方式1：初始化默认客户端（推荐）
	config := &RabbitMQConfig{
		Host:     "192.168.165.88",
		Port:     15672,
		Username: "admin",
		Password: "123456",
		Vhost:    "mq-auto-scale",
	}

	InitDefaultClient(config)
	client := GetDefaultClient()

	// 复用同一个客户端多次查询
	for i := range 5 {
		queueInfo, err := client.GetQueueInfo("mq-auto-scale", "ama.message.pull")
		if err != nil {
			log.Printf("查询失败: %v", err)
			continue
		}
		fmt.Printf("第%d次查询 - 消息数: %d\n", i+1, queueInfo.Messages)
		t.Logf("第%d次查询 - 消息数: %d\n", i+1, queueInfo.Messages)
		time.Sleep(time.Second)
	}

	// 方式2：使用缓存客户端（适用于多配置场景）
	config1 := &RabbitMQConfig{
		Host:     "192.168.165.88",
		Port:     15672,
		Username: "admin",
		Password: "123456",
	}

	client1 := GetOrCreateClient(config1)
	client2 := GetOrCreateClient(config1) // 返回缓存的客户端实例

	fmt.Printf("client1 == client2: %v\n", client1 == client2) // true

	// 方式3：手动创建客户端（不推荐，除非特殊需求）
	manualClient := NewRabbitMQClient(config)
	defer manualClient.Close()

	// 并发安全测试
	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			stats, err := client.GetQueueStats("mq-auto-scale", "ama.message.pull")
			if err == nil {
				fmt.Printf("Goroutine %d: 消息数=%d\n", id, stats.Messages)
				t.Logf("Goroutine %d: 消息数=%d\n", id, stats.Messages)
			}
		}(i)
	}
	wg.Wait()

	// 程序退出前清理资源
	defer ClearClients()
}
