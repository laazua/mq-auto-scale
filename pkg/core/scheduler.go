// pkg/core/scheduler.go
package core

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// SchedulerConfig 调度器配置
type SchedulerConfig struct {
	// RabbitMQ配置
	RabbitMQConfig *RabbitMQConfig

	// Supervisor配置
	SupervisorConfig *SupervisorConfig

	// 监控配置
	MonitorConfig *MonitorConfig

	// 调度间隔
	CheckInterval time.Duration

	// 扩容阈值配置
	ScaleUpConfig ScaleConfig

	// 缩容阈值配置
	ScaleDownConfig ScaleConfig

	// 队列与程序名称的映射关系
	// key: 队列名称, value: supervisor程序名称
	QueueProgramMapping map[string]string

	// 是否需要自动缩容
	EnableAutoScaleDown bool

	// 扩容冷却时间（防止频繁扩容）
	ScaleUpCooldown time.Duration

	// 缩容冷却时间
	ScaleDownCooldown time.Duration
}

// ScaleConfig 扩缩容配置
type ScaleConfig struct {
	// 消息积压阈值（待消费消息数超过此值触发扩容）
	MessageBacklogThreshold int

	// 消费者利用率阈值（低于此值触发缩容）
	ConsumerUtilisationThreshold float64

	// 每次扩容增加的进程数
	ScaleUpStep int

	// 每次缩容减少的进程数
	ScaleDownStep int

	// 最小进程数
	MinProcesses int

	// 最大进程数
	MaxProcesses int
}

// DefaultSchedulerConfig 默认调度器配置
func DefaultSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		CheckInterval: 30 * time.Second,
		ScaleUpConfig: ScaleConfig{
			MessageBacklogThreshold:      1000,
			ConsumerUtilisationThreshold: 0.5,
			ScaleUpStep:                  2,
			ScaleDownStep:                1,
			MinProcesses:                 1,
			MaxProcesses:                 20,
		},
		ScaleDownConfig: ScaleConfig{
			MessageBacklogThreshold:      100,
			ConsumerUtilisationThreshold: 0.3,
			ScaleUpStep:                  0,
			ScaleDownStep:                1,
			MinProcesses:                 1,
			MaxProcesses:                 20,
		},
		QueueProgramMapping: make(map[string]string),
		EnableAutoScaleDown: true,
		ScaleUpCooldown:     2 * time.Minute,
		ScaleDownCooldown:   5 * time.Minute,
	}
}

// QueueStatus 队列状态
type QueueStatus struct {
	Name                string
	ProgramName         string
	MessagesReady       int
	Consumers           int
	ConsumerUtilisation float64
	CurrentProcesses    int
	TargetProcesses     int
	NeedScaleUp         bool
	NeedScaleDown       bool
	ScaleUpReason       string
	ScaleDownReason     string
	LastScaleUpTime     time.Time
	LastScaleDownTime   time.Time
}

// Scheduler 调度器
type Scheduler struct {
	config       *SchedulerConfig
	rabbitClient *RabbitMQClient
	supervisor   *SupervisorManage
	monitor      *HostMonitor
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup

	// 记录各队列最后扩缩容时间
	lastScaleUpTime   map[string]time.Time
	lastScaleDownTime map[string]time.Time

	// 调度统计信息
	stats *SchedulerStats
}

// SchedulerStats 调度器统计信息
type SchedulerStats struct {
	TotalChecks       int64
	TotalScaleUp      int64
	TotalScaleDown    int64
	FailedOperations  int64
	LastCheckTime     time.Time
	LastScaleUpTime   time.Time
	LastScaleDownTime time.Time
	mu                sync.RWMutex
}

// NewScheduler 创建调度器
func NewScheduler(config *SchedulerConfig) (*Scheduler, error) {
	if config == nil {
		config = DefaultSchedulerConfig()
	}

	// 初始化RabbitMQ客户端
	rabbitClient := GetOrCreateClient(config.RabbitMQConfig)

	// 初始化Supervisor管理器
	supervisor := NewSupervisorManage(config.SupervisorConfig)

	// 初始化监控器
	monitor := NewHostMonitor(config.MonitorConfig)

	ctx, cancel := context.WithCancel(context.Background())

	scheduler := &Scheduler{
		config:            config,
		rabbitClient:      rabbitClient,
		supervisor:        supervisor,
		monitor:           monitor,
		ctx:               ctx,
		cancel:            cancel,
		lastScaleUpTime:   make(map[string]time.Time),
		lastScaleDownTime: make(map[string]time.Time),
		stats:             &SchedulerStats{},
	}

	return scheduler, nil
}

// Start 启动调度器
func (s *Scheduler) Start() error {
	slog.Info("[scheduler] 启动调度器",
		"轮询检查时间间隔", s.config.CheckInterval,
		"是否自动进行扩缩容", s.config.EnableAutoScaleDown)

	s.wg.Add(1)
	go s.scheduleLoop()

	return nil
}

// Stop 停止调度器
func (s *Scheduler) Stop() {
	slog.Info("[scheduler] 正在关闭调度器")
	s.cancel()
	s.wg.Wait()
	slog.Info("[scheduler] 调度器关闭完成")
}

// scheduleLoop 调度循环
func (s *Scheduler) scheduleLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.performCheck()
		}
	}
}

// performCheck 执行一次检查
func (s *Scheduler) performCheck() {
	slog.Debug("[scheduler] 执行调度器检测")

	// 更新统计
	s.stats.mu.Lock()
	s.stats.TotalChecks++
	s.stats.LastCheckTime = time.Now()
	s.stats.mu.Unlock()

	// 1. 检查主机状态
	suitability, err := s.monitor.CheckSuitability()
	if err != nil {
		slog.Error("[scheduler] 检测主机状态失败", "error", err)
		s.recordFailure()
		return
	}

	slog.Debug("[scheduler] 主机状态",
		"可以进行扩容", suitability.CanStart,
		"主机健康得分", suitability.Score,
		"cpu", suitability.Metrics.CPUUsagePercent,
		"内存", suitability.Metrics.MemoryUsagePercent,
		"负载", suitability.Metrics.LoadAvg1min)

	// 2. 获取所有需要监控的队列
	queuesToCheck := s.getQueuesToCheck()

	// 3. 检查每个队列并决定扩缩容
	var scaleOperations []func() error

	for queueName, programName := range queuesToCheck {
		status, err := s.analyzeQueue(queueName, programName)
		if err != nil {
			slog.Error("[scheduler] 分析队列失败",
				"队列名", queueName,
				"进程名", programName,
				"error", err)
			continue
		}

		// 记录队列状态
		slog.Info("[scheduler] 队列状态",
			"队列名", status.Name,
			"进程名", status.ProgramName,
			"就绪消息", status.MessagesReady,
			"当前消费者数量", status.Consumers,
			"当前进程数量", status.CurrentProcesses,
			"目标进程数量", status.TargetProcesses,
			"需要扩容", status.NeedScaleUp,
			"需要缩容", status.NeedScaleDown)

		// 决定扩缩容操作
		if status.NeedScaleUp {
			// 只有在主机状态适合时才扩容
			if suitability.CanStart {
				if s.canScaleUp(programName) {
					scaleOps := s.buildScaleUpOperation(programName, status.TargetProcesses)
					scaleOperations = append(scaleOperations, scaleOps...)
				} else {
					slog.Info("[scheduler] 冷却跳过扩容", "进程名", programName)
				}
			} else {
				slog.Info("[scheduler] 主机节点不允许扩容",
					"程序名", programName,
					"原因", suitability.Reason)
			}
		} else if status.NeedScaleDown && s.config.EnableAutoScaleDown {
			if s.canScaleDown(programName) {
				scaleOps := s.buildScaleDownOperation(programName, status.TargetProcesses)
				scaleOperations = append(scaleOperations, scaleOps...)
			} else {
				slog.Info("[scheduler] 冷却跳过缩容", "程序名", programName)
			}
		}
	}

	// 4. 执行扩缩容操作
	for _, op := range scaleOperations {
		if err := op(); err != nil {
			slog.Error("[scheduler] 执行扩缩容失败", "error", err)
			s.recordFailure()
		}
	}

	slog.Debug("[scheduler] 调度器检测完成")
}

// getQueuesToCheck 获取需要检查的队列
func (s *Scheduler) getQueuesToCheck() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.config.QueueProgramMapping) > 0 {
		return s.config.QueueProgramMapping
	}

	// 如果没有配置映射，尝试从supervisor获取所有程序并推断队列名
	programs := s.supervisor.GetAllPrograms()
	mapping := make(map[string]string)
	for name, config := range programs {
		if config.QueueName != "" {
			mapping[config.QueueName] = name
		} else {
			mapping[name] = name
		}
	}
	return mapping
}

// analyzeQueue 分析队列状态
func (s *Scheduler) analyzeQueue(queueName, programName string) (*QueueStatus, error) {
	// 获取队列信息
	vhost := s.config.RabbitMQConfig.Vhost
	if vhost == "" {
		vhost = "/"
	}
	queueInfo, err := s.rabbitClient.GetQueueInfo(vhost, queueName)
	if err != nil {
		return nil, fmt.Errorf("获取队列信息失败: %w", err)
	}

	// 获取当前运行的进程数
	currentProcesses, err := s.supervisor.GetRunningConsumerCount(programName)
	if err != nil {
		slog.Warn("[scheduler] 获取运行中的消费者数量失败",
			"进程名", programName,
			"error", err)
		// 降级：使用配置文件中的值
		if programConfig, err := s.supervisor.GetProgramConfig(programName); err == nil {
			currentProcesses = programConfig.CurrentCount
		}
	}

	status := &QueueStatus{
		Name:                queueName,
		ProgramName:         programName,
		MessagesReady:       queueInfo.MessagesReady,
		Consumers:           queueInfo.Consumers,
		ConsumerUtilisation: queueInfo.ConsumerUtilisation,
		CurrentProcesses:    currentProcesses,
		TargetProcesses:     currentProcesses,
	}

	// 获取最后扩缩容时间
	s.mu.RLock()
	status.LastScaleUpTime = s.lastScaleUpTime[programName]
	status.LastScaleDownTime = s.lastScaleDownTime[programName]
	s.mu.RUnlock()

	// 判断是否需要扩容
	status.NeedScaleUp, status.ScaleUpReason = s.shouldScaleUp(status)

	// 判断是否需要缩容
	if !status.NeedScaleUp {
		status.NeedScaleDown, status.ScaleDownReason = s.shouldScaleDown(status)
	}

	// 计算目标进程数
	if status.NeedScaleUp {
		status.TargetProcesses = s.calculateTargetScaleUp(currentProcesses)
	} else if status.NeedScaleDown {
		status.TargetProcesses = s.calculateTargetScaleDown(currentProcesses)
	}

	return status, nil
}

// shouldScaleUp 判断是否需要扩容
func (s *Scheduler) shouldScaleUp(status *QueueStatus) (bool, string) {
	// 检查是否已达到最大进程数
	if status.CurrentProcesses >= s.config.ScaleUpConfig.MaxProcesses {
		return false, fmt.Sprintf("已经达到最大进程数量: %d", status.CurrentProcesses)
	}

	// 检查消息积压
	if status.MessagesReady > s.config.ScaleUpConfig.MessageBacklogThreshold {
		return true, fmt.Sprintf("message backlog %d exceeds threshold %d",
			status.MessagesReady, s.config.ScaleUpConfig.MessageBacklogThreshold)
	}

	// 检查消费者利用率（如果利用率低但消息积压多，也可能需要扩容）
	if status.ConsumerUtilisation > 0 &&
		status.ConsumerUtilisation > s.config.ScaleUpConfig.ConsumerUtilisationThreshold &&
		status.MessagesReady > s.config.ScaleUpConfig.MessageBacklogThreshold/2 {
		return true, fmt.Sprintf("high consumer utilisation %.2f with backlog %d",
			status.ConsumerUtilisation, status.MessagesReady)
	}

	// 检查每个消费者的平均消息积压
	if status.Consumers > 0 {
		avgBacklogPerConsumer := status.MessagesReady / status.Consumers
		if avgBacklogPerConsumer > s.config.ScaleUpConfig.MessageBacklogThreshold {
			return true, fmt.Sprintf("average backlog per consumer %d exceeds threshold",
				avgBacklogPerConsumer)
		}
	}

	return false, "no scale up needed"
}

// shouldScaleDown 判断是否需要缩容
func (s *Scheduler) shouldScaleDown(status *QueueStatus) (bool, string) {
	// 检查是否已达到最小进程数
	if status.CurrentProcesses <= s.config.ScaleDownConfig.MinProcesses {
		return false, fmt.Sprintf("already at min processes: %d", status.CurrentProcesses)
	}

	// 检查消息积压是否低于缩容阈值
	if status.MessagesReady <= s.config.ScaleDownConfig.MessageBacklogThreshold {
		// 检查消费者利用率是否过低
		if status.ConsumerUtilisation < s.config.ScaleDownConfig.ConsumerUtilisationThreshold {
			return true, fmt.Sprintf("low message backlog %d and low consumer utilisation %.2f",
				status.MessagesReady, status.ConsumerUtilisation)
		}

		// 如果消息很少，即使利用率不低也可以缩容
		if status.MessagesReady < s.config.ScaleDownConfig.MessageBacklogThreshold/2 {
			return true, fmt.Sprintf("very low message backlog %d", status.MessagesReady)
		}
	}

	return false, "no scale down needed"
}

// calculateTargetScaleUp 计算扩容目标进程数
func (s *Scheduler) calculateTargetScaleUp(current int) int {
	target := min(current+s.config.ScaleUpConfig.ScaleUpStep, s.config.ScaleUpConfig.MaxProcesses)
	return target
}

// calculateTargetScaleDown 计算缩容目标进程数
func (s *Scheduler) calculateTargetScaleDown(current int) int {
	target := current - s.config.ScaleDownConfig.ScaleDownStep
	if target < s.config.ScaleDownConfig.MinProcesses {
		target = s.config.ScaleDownConfig.MinProcesses
	}
	return target
}

// canScaleUp 检查是否可以扩容（冷却时间检查）
func (s *Scheduler) canScaleUp(programName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lastTime, exists := s.lastScaleUpTime[programName]
	if !exists {
		return true
	}

	return time.Since(lastTime) >= s.config.ScaleUpCooldown
}

// canScaleDown 检查是否可以缩容（冷却时间检查）
func (s *Scheduler) canScaleDown(programName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lastTime, exists := s.lastScaleDownTime[programName]
	if !exists {
		return true
	}

	return time.Since(lastTime) >= s.config.ScaleDownCooldown
}

// buildScaleUpOperation 构建扩容操作
func (s *Scheduler) buildScaleUpOperation(programName string, targetProcesses int) []func() error {
	var operations []func() error

	operations = append(operations, func() error {
		slog.Info("[scheduler] 扩容消费者",
			"程序名", programName,
			"目标进程数量", targetProcesses)

		// 记录扩容时间
		s.mu.Lock()
		s.lastScaleUpTime[programName] = time.Now()
		s.mu.Unlock()

		// 更新统计
		s.stats.mu.Lock()
		s.stats.TotalScaleUp++
		s.stats.LastScaleUpTime = time.Now()
		s.stats.mu.Unlock()

		// 执行扩容
		err := s.supervisor.UpdateConsumerCount(programName, targetProcesses)
		if err != nil {
			slog.Error("[scheduler] 扩容失败", "进程名", programName, "error", err)
			return err
		}

		slog.Info("[scheduler] 扩容完成", "进程名", programName, "目标进程数量", targetProcesses)
		return nil
	})

	return operations
}

// buildScaleDownOperation 构建缩容操作
func (s *Scheduler) buildScaleDownOperation(programName string, targetProcesses int) []func() error {
	var operations []func() error

	operations = append(operations, func() error {
		slog.Info("[scheduler] 缩容消费者",
			"进程名", programName,
			"目标进程数量", targetProcesses)

		// 记录缩容时间
		s.mu.Lock()
		s.lastScaleDownTime[programName] = time.Now()
		s.mu.Unlock()

		// 更新统计
		s.stats.mu.Lock()
		s.stats.TotalScaleDown++
		s.stats.LastScaleDownTime = time.Now()
		s.stats.mu.Unlock()

		// 执行缩容
		err := s.supervisor.UpdateConsumerCount(programName, targetProcesses)
		if err != nil {
			slog.Error("[scheduler] 缩容失败", "进程名", programName, "error", err)
			return err
		}

		slog.Info("[scheduler] 缩容完成", "进程名", programName, "目标进程数量", targetProcesses)
		return nil
	})

	return operations
}

// recordFailure 记录失败操作
func (s *Scheduler) recordFailure() {
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()
	s.stats.FailedOperations++
}

// GetStats 获取调度器统计信息
func (s *Scheduler) GetStats() *SchedulerStats {
	s.stats.mu.RLock()
	defer s.stats.mu.RUnlock()

	statsCopy := *s.stats
	return &statsCopy
}

// GetQueueStatus 获取指定队列的状态
func (s *Scheduler) GetQueueStatus(queueName string) (*QueueStatus, error) {
	programName, exists := s.config.QueueProgramMapping[queueName]
	if !exists {
		// 尝试从所有程序中查找
		programs := s.supervisor.GetAllPrograms()
		for pName, pConfig := range programs {
			if pConfig.QueueName == queueName || pName == queueName {
				programName = pName
				exists = true
				break
			}
		}
	}

	if !exists {
		return nil, fmt.Errorf("当前虚拟机没有存在的队列进行调度: %s", queueName)
	}

	return s.analyzeQueue(queueName, programName)
}

// GetAllQueueStatus 获取所有队列的状态
func (s *Scheduler) GetAllQueueStatus() (map[string]*QueueStatus, error) {
	queues := s.getQueuesToCheck()
	statuses := make(map[string]*QueueStatus)

	for queueName, programName := range queues {
		status, err := s.analyzeQueue(queueName, programName)
		if err != nil {
			slog.Warn("[scheduler] 获取队列状态失败",
				"queue", queueName,
				"error", err)
			continue
		}
		statuses[queueName] = status
	}

	return statuses, nil
}

// ManualScale 手动扩缩容
func (s *Scheduler) ManualScale(programName string, targetProcesses int) error {
	slog.Info("[scheduler] 手动扩容",
		"进程名", programName,
		"目标进程数量", targetProcesses)

	// 获取程序配置
	programConfig, err := s.supervisor.GetProgramConfig(programName)
	if err != nil {
		return fmt.Errorf("获取进程配置失败: %w", err)
	}

	// 验证目标数量
	if targetProcesses < programConfig.MinCount {
		targetProcesses = programConfig.MinCount
		slog.Info("[scheduler] 当前进程数量达到最少配置的目标进程数量", "配置的最少目标进程数量", programConfig.MinCount)
	}
	if targetProcesses > programConfig.MaxCount {
		targetProcesses = programConfig.MaxCount
		slog.Info("[scheduler] 当前进程数量达到最多配置的目标进程数量", "配置的最多目标进程数量", programConfig.MaxCount)
	}

	// 执行更新
	err = s.supervisor.UpdateConsumerCount(programName, targetProcesses)
	if err != nil {
		slog.Error("[scheduler] 手动扩缩容失败", "进程名", programName, "error", err)
		return err
	}

	slog.Info("[scheduler] 手动扩缩容成功", "进程名", programName, "target", targetProcesses)
	return nil
}

// UpdateQueueMapping 更新队列映射
func (s *Scheduler) UpdateQueueMapping(mapping map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config.QueueProgramMapping = mapping
	slog.Info("[scheduler]  更新队列成功", "mapping", mapping)
}

// UpdateConfig 更新调度器配置
func (s *Scheduler) UpdateConfig(config *SchedulerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = config
	slog.Info("[scheduler] 更新队列配置更改")
}
