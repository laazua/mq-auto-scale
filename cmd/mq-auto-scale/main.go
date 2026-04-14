package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mq-auto-scale/pkg/comm"
	"mq-auto-scale/pkg/core"
)

func main() {
	// 加载配置
	if err := comm.LoadConfig(); err != nil {
		panic(err)
	}
	// 加载日志设置
	if err := comm.SetupLog(); err != nil {
		panic(err)
	}
	// Rabbitmq 配置
	mqConfig := &core.RabbitMQConfig{
		Host:     comm.Config().Mq.Host,
		Port:     comm.Config().Mq.Port,
		Username: comm.Config().Mq.Username,
		Password: comm.Config().Mq.Password,
		Vhost:    comm.Config().Mq.Vhost,
	}
	// supervisor配置
	supervisorConfig := &core.SupervisorConfig{
		URL:       comm.Config().Supervisor.Url,
		Username:  comm.Config().Supervisor.User,
		Password:  comm.Config().Supervisor.Pass,
		Timeout:   comm.Config().Supervisor.TimeOut, // 增加到120秒，与OpTimeout一致
		OpTimeout: comm.Config().Supervisor.OpTimeOut,
		ConfigDir: comm.Config().Supervisor.ConfigPath,
	}
	// 主机监控配置
	nodeMonitorConfig := &core.MonitorConfig{
		MaxCPUUsagePercent:    80.0,
		MaxMemoryUsagePercent: 70.0,
		MaxLoadPerCore:        1.0,
		CheckInterval:         5 * time.Second,
		ConsecutiveChecks:     3,
	}
	// 3. 配置调度器
	schedulerConfig := &core.SchedulerConfig{
		RabbitMQConfig:      mqConfig,
		SupervisorConfig:    supervisorConfig,
		MonitorConfig:       nodeMonitorConfig,
		CheckInterval:       comm.Config().ScheduleInterval,
		QueueProgramMapping: comm.Config().ScheduleQueueProgramMapping,
		ScaleUpConfig: core.ScaleConfig{
			MessageBacklogThreshold:      comm.Config().ScheduleScaleUpConfigScaleThreshold,               // 消息积压超过500触发扩容
			ConsumerUtilisationThreshold: comm.Config().ScheduleScaleUpConfigConsumerUtilisationThreshold, // 消费者利用率超过60%
			ScaleUpStep:                  comm.Config().ScheduleScaleUpConfigScaleUpStep,                  // 每次增加2个进程
			ScaleDownStep:                comm.Config().ScheduleScaleUpConfigScaleDownStep,                // 每次减少1个进程
			MinProcesses:                 comm.Config().Supervisor.MinProcess,                             // 最少1个进程
			MaxProcesses:                 comm.Config().Supervisor.MaxProcess,                             // 最多20个进程
		},
		ScaleDownConfig: core.ScaleConfig{
			MessageBacklogThreshold:      comm.Config().ScheduleScaleDownConfigScaleThreshold,               // 消息积压低于50触发缩容
			ConsumerUtilisationThreshold: comm.Config().ScheduleScaleDownConfigConsumerUtilisationThreshold, // 消费者利用率低于20%
			ScaleDownStep:                comm.Config().ScheduleScaleDownConfigScaleDownStep,
			MinProcesses:                 comm.Config().Supervisor.MinProcess,
			MaxProcesses:                 comm.Config().Supervisor.MaxProcess,
		},
		EnableAutoScaleDown: true,
		ScaleUpCooldown:     2 * time.Minute, // 扩容冷却2分钟
		ScaleDownCooldown:   5 * time.Minute, // 缩容冷却5分钟
	}

	// 4. 创建调度器
	scheduler, err := core.NewScheduler(schedulerConfig)
	if err != nil {
		slog.Error("创建调度器失败", "error", err)
		os.Exit(1)
	}

	// 5. 启动调度器
	if err := scheduler.Start(); err != nil {
		slog.Error("启动调度器失败", "error", err)
		os.Exit(1)
	}

	// 6. 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	// 8. 停止调度器
	scheduler.Stop()
}
