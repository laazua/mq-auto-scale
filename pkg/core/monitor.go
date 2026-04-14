package core

import (
	"fmt"
	"log"
	"log/slog"
	"math"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

// HostMetrics 存储主机指标
type HostMetrics struct {
	CPUUsagePercent    float64 // CPU 使用率百分比 (0-100)
	MemoryUsagePercent float64 // 内存使用率百分比 (0-100)
	LoadAvg1min        float64 // 1分钟平均负载
	LoadAvg5min        float64 // 5分钟平均负载
	LoadAvg15min       float64 // 15分钟平均负载
	CPUCores           int     // CPU 核心数
}

// TaskSuitability 任务启动适宜性结果
type TaskSuitability struct {
	CanStart bool        // 是否可以启动任务
	Reason   string      // 主要原因
	Details  []string    // 详细原因列表
	Metrics  HostMetrics // 当前指标快照
	Score    float64     // 适宜性评分 (0-100, 越高越适合)
}

// MonitorConfig 监控器配置
type MonitorConfig struct {
	MaxCPUUsagePercent    float64       // 最大允许 CPU 使用率 (%)
	MaxMemoryUsagePercent float64       // 最大允许内存使用率 (%)
	MaxLoadPerCore        float64       // 每核心最大负载阈值
	CheckInterval         time.Duration // 检查间隔
	ConsecutiveChecks     int           // 需要连续满足条件的次数
}

// DefaultMonitorConfig 默认配置
func DefaultMonitorConfig() MonitorConfig {
	return MonitorConfig{
		MaxCPUUsagePercent:    70.0,
		MaxMemoryUsagePercent: 80.0,
		MaxLoadPerCore:        1.0,
		CheckInterval:         5 * time.Second,
		ConsecutiveChecks:     3,
	}
}

// HostMonitor 主机监控器
type HostMonitor struct {
	config      *MonitorConfig
	consecutive int
}

// NewHostMonitor 创建主机监控器
func NewHostMonitor(config *MonitorConfig) *HostMonitor {
	return &HostMonitor{
		config:      config,
		consecutive: 0,
	}
}

// CollectMetrics 收集主机指标
func (m *HostMonitor) CollectMetrics() (*HostMetrics, error) {
	metrics := &HostMetrics{}

	// 获取 CPU 核心数
	metrics.CPUCores = runtime.NumCPU()

	// 获取 CPU 使用率
	cpuPercent, err := cpu.Percent(time.Second, false)
	if err != nil {
		return nil, fmt.Errorf("获取 CPU 使用率失败: %v", err)
	}
	if len(cpuPercent) > 0 {
		metrics.CPUUsagePercent = cpuPercent[0]
	}

	// 获取内存使用率
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, fmt.Errorf("获取内存信息失败: %v", err)
	}
	metrics.MemoryUsagePercent = memInfo.UsedPercent

	// 获取系统负载
	loadAvg, err := load.Avg()
	if err != nil {
		return nil, fmt.Errorf("获取系统负载失败: %v", err)
	}
	metrics.LoadAvg1min = loadAvg.Load1
	metrics.LoadAvg5min = loadAvg.Load5
	metrics.LoadAvg15min = loadAvg.Load15

	return metrics, nil
}

// CheckSuitability 检查是否适合启动任务
func (m *HostMonitor) CheckSuitability() (*TaskSuitability, error) {
	metrics, err := m.CollectMetrics()
	if err != nil {
		return nil, err
	}

	suitability := &TaskSuitability{
		CanStart: true,
		Metrics:  *metrics,
		Details:  []string{},
	}

	// 计算负载阈值（考虑核心数）
	maxLoad := float64(metrics.CPUCores) * m.config.MaxLoadPerCore
	loadCheck := metrics.LoadAvg1min <= maxLoad

	// 检查各项指标
	if metrics.CPUUsagePercent > m.config.MaxCPUUsagePercent {
		suitability.CanStart = false
		suitability.Details = append(suitability.Details,
			fmt.Sprintf("CPU 使用率过高: %.2f%% > %.0f%%",
				metrics.CPUUsagePercent, m.config.MaxCPUUsagePercent))
	}

	if metrics.MemoryUsagePercent > m.config.MaxMemoryUsagePercent {
		suitability.CanStart = false
		suitability.Details = append(suitability.Details,
			fmt.Sprintf("内存使用率过高: %.2f%% > %.0f%%",
				metrics.MemoryUsagePercent, m.config.MaxMemoryUsagePercent))
	}

	if !loadCheck {
		suitability.CanStart = false
		suitability.Details = append(suitability.Details,
			fmt.Sprintf("系统负载过高: %.2f > %.2f (核心数: %d, 阈值每核心: %.1f)",
				metrics.LoadAvg1min, maxLoad, metrics.CPUCores, m.config.MaxLoadPerCore))
	}

	// 计算适宜性评分
	suitability.Score = m.calculateScore(metrics)

	// 设置主要原因
	if suitability.CanStart {
		suitability.Reason = "主机资源充足，适合启动任务"
	} else {
		suitability.Reason = "主机资源不足，不适合启动任务"
		if len(suitability.Details) > 0 {
			suitability.Reason = suitability.Details[0]
		}
	}

	return suitability, nil
}

// calculateScore 计算适宜性评分 (0-100)
func (m *HostMonitor) calculateScore(metrics *HostMetrics) float64 {
	// 各指标权重
	cpuWeight := 0.4
	memWeight := 0.3
	loadWeight := 0.3

	// CPU 得分 (使用率越低得分越高)
	cpuScore := math.Max(0, 100*(1-metrics.CPUUsagePercent/100))

	// 内存得分
	memScore := math.Max(0, 100*(1-metrics.MemoryUsagePercent/100))

	// 负载得分
	maxLoad := float64(metrics.CPUCores) * m.config.MaxLoadPerCore
	loadRatio := math.Min(1.0, metrics.LoadAvg1min/maxLoad)
	loadScore := math.Max(0, 100*(1-loadRatio))

	// 加权总分
	totalScore := cpuScore*cpuWeight + memScore*memWeight + loadScore*loadWeight

	return math.Min(100, math.Max(0, totalScore))
}

// WaitForSuitable 等待直到适合启动任务（带连续检查）
func (m *HostMonitor) WaitForSuitable() (*TaskSuitability, error) {
	ticker := time.NewTicker(m.config.CheckInterval)
	defer ticker.Stop()

	for {
		suitability, err := m.CheckSuitability()
		if err != nil {
			return nil, err
		}

		if suitability.CanStart {
			m.consecutive++
			if m.consecutive >= m.config.ConsecutiveChecks {
				slog.Info("[monitor] 可以启动任务", "节点健康度检查次数", m.consecutive)
				return suitability, nil
			}
			log.Printf("[monitor] 检查通过 (%d/%d): CPU=%.1f%%, 内存=%.1f%%, 负载=%.2f",
				m.consecutive, m.config.ConsecutiveChecks,
				suitability.Metrics.CPUUsagePercent,
				suitability.Metrics.MemoryUsagePercent,
				suitability.Metrics.LoadAvg1min)
		} else {
			m.consecutive = 0
			log.Printf("[monitor] 检查失败: %s", suitability.Reason)
		}

		<-ticker.C
	}
}

// MonitorAndDecide 监控并决定是否启动任务（单次检查）
func (m *HostMonitor) MonitorAndDecide() (*TaskSuitability, error) {
	return m.CheckSuitability()
}
