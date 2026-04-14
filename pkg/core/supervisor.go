// supervisor管理
package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SupervisorConfig Supervisor RPC 配置
type SupervisorConfig struct {
	URL       string // XML-RPC 地址，如 http://localhost:9001/RPC2
	Username  string
	Password  string
	Timeout   time.Duration // RPC 调用超时时间
	OpTimeout time.Duration // 操作超时时间（重启等操作）
	ConfigDir string        // Supervisor 配置文件目录
}

// ProgramConfig 程序配置信息
type ProgramConfig struct {
	ConfigPath   string // 配置文件路径
	ProgramName  string // 程序名称
	CurrentCount int    // 当前 numprocs 值
	MinCount     int    // 最小进程数
	MaxCount     int    // 最大进程数
	QueueName    string // 对应的队列名称（从配置中读取或自动映射）
}

// XML-RPC 结构体定义
type methodCall struct {
	XMLName    xml.Name     `xml:"methodCall"`
	MethodName string       `xml:"methodName"`
	Params     methodParams `xml:"params"`
}

type methodParams struct {
	Param []methodParam `xml:"param"`
}

type methodParam struct {
	Value rpcValue `xml:"value"`
}

type rpcValue struct {
	Int     *int       `xml:"int"`
	I4      *int       `xml:"i4"`
	String  *string    `xml:"string"`
	Boolean *bool      `xml:"boolean"`
	Struct  *rpcStruct `xml:"struct"`
	Array   *rpcArray  `xml:"array"`
}

type rpcStruct struct {
	Member []rpcMember `xml:"member"`
}

type rpcMember struct {
	Name  string   `xml:"name"`
	Value rpcValue `xml:"value"`
}

type rpcArray struct {
	Data struct {
		Value []rpcValue `xml:"value"`
	} `xml:"data"`
}

type methodResponse struct {
	XMLName xml.Name      `xml:"methodResponse"`
	Params  *methodParams `xml:"params"`
	Fault   *rpcFault     `xml:"fault"`
}

type rpcFault struct {
	Value rpcValue `xml:"value"`
}

// ProcessInfo 进程信息
type ProcessInfo struct {
	Name        string
	Group       string
	State       int // 0: stopped, 10: starting, 20: running, 100: stopped
	Description string
	PID         int
	ExitStatus  int
}

// SupervisorManage Supervisor XML-RPC 客户端
type SupervisorManage struct {
	config     *SupervisorConfig
	client     *http.Client
	mu         sync.Mutex
	programs   map[string]*ProgramConfig // 程序名称 -> 配置
	programsMu sync.RWMutex
}

// NewSupervisorManage 创建 Supervisor 客户端
func NewSupervisorManage(config *SupervisorConfig) *SupervisorManage {
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.OpTimeout == 0 {
		config.OpTimeout = 120 * time.Second
	}
	if config.ConfigDir == "" {
		config.ConfigDir = "/etc/supervisor/conf.d"
	}

	manager := &SupervisorManage{
		config:   config,
		client:   &http.Client{Timeout: config.Timeout},
		programs: make(map[string]*ProgramConfig),
	}

	// 加载所有程序配置
	manager.programsMu.Lock()
	defer manager.programsMu.Unlock()
	if err := manager.loadAllPrograms(); err != nil {
		slog.Warn("[supervisor]  加载进程配置文件失败", "error", err)
	}

	return manager
}

// loadAllPrograms 加载所有程序配置（调用者需要持有锁）
func (c *SupervisorManage) loadAllPrograms() error {
	files, err := filepath.Glob(filepath.Join(c.config.ConfigDir, "*.ini"))
	if err != nil {
		return fmt.Errorf("failed to glob config files: %w", err)
	}

	for _, file := range files {
		programConfig, err := c.parseProgramConfig(file)
		if err != nil {
			slog.Warn("[supervisor] 解析进程配置失败", "进程配置文件", file, "error", err)
			continue
		}

		c.programs[programConfig.ProgramName] = programConfig

		slog.Info("[supervisor] 加载进程配置", "进程名", programConfig.ProgramName, "配置文件", file,
			"当前进程数量", programConfig.CurrentCount, "最少进程数量", programConfig.MinCount, "最多进程数量", programConfig.MaxCount)
	}

	return nil
}

// RefreshPrograms 刷新程序配置
func (c *SupervisorManage) RefreshPrograms() error {
	c.programsMu.Lock()
	defer c.programsMu.Unlock()

	// 清空现有配置
	c.programs = make(map[string]*ProgramConfig)

	// 重新加载
	return c.loadAllPrograms()
}

// parseProgramConfig 解析程序配置文件
func (c *SupervisorManage) parseProgramConfig(configPath string) (*ProgramConfig, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	config := &ProgramConfig{
		ConfigPath:   configPath,
		CurrentCount: 1,
		MinCount:     1,
		MaxCount:     20,
	}

	scanner := bufio.NewScanner(file)
	var currentProgramName string
	var inProgramSection bool

	for scanner.Scan() {
		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)

		// 跳过空行和注释
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, ";") || strings.HasPrefix(trimmedLine, "#") {
			continue
		}

		// 检查是否是 program section
		if strings.HasPrefix(trimmedLine, "[program:") && strings.HasSuffix(trimmedLine, "]") {
			// 如果之前已经在 program section 中，说明解析完成
			if inProgramSection && currentProgramName != "" {
				break
			}

			// 提取 program 名称
			currentProgramName = strings.TrimPrefix(trimmedLine, "[program:")
			currentProgramName = strings.TrimSuffix(currentProgramName, "]")
			currentProgramName = strings.TrimSpace(currentProgramName)
			inProgramSection = true
			config.ProgramName = currentProgramName
			continue
		}

		// 如果遇到其他 section，退出当前 program 解析
		if strings.HasPrefix(trimmedLine, "[") && strings.HasSuffix(trimmedLine, "]") {
			if inProgramSection && config.ProgramName != "" {
				break
			}
			continue
		}

		// 只在 program section 内解析配置
		if !inProgramSection || config.ProgramName == "" {
			continue
		}

		// 解析 key=value 格式
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "numprocs":
			if num, err := strconv.Atoi(value); err == nil {
				config.CurrentCount = num
			}
		case "minprocs":
			if num, err := strconv.Atoi(value); err == nil {
				config.MinCount = num
			}
		case "maxprocs":
			if num, err := strconv.Atoi(value); err == nil {
				config.MaxCount = num
			}
		case "command":
			config.QueueName = c.extractQueueName(value)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	if config.ProgramName == "" {
		return nil, fmt.Errorf("program name not found in config")
	}

	return config, nil
}

// extractQueueName 从命令中提取队列名称
func (c *SupervisorManage) extractQueueName(command string) string {
	parts := strings.Fields(command)
	for i, part := range parts {
		if strings.Contains(part, "pull") && i > 0 {
			return parts[i-1]
		}
	}
	return "default"
}

// GetAllPrograms 获取所有程序配置
func (c *SupervisorManage) GetAllPrograms() map[string]*ProgramConfig {
	c.programsMu.RLock()
	defer c.programsMu.RUnlock()

	result := make(map[string]*ProgramConfig, len(c.programs))
	for k, v := range c.programs {
		result[k] = v
	}
	return result
}

// GetProgramConfig 获取指定程序配置
func (c *SupervisorManage) GetProgramConfig(programName string) (*ProgramConfig, error) {
	c.programsMu.RLock()
	defer c.programsMu.RUnlock()

	if config, ok := c.programs[programName]; ok {
		// 返回副本避免外部修改
		configCopy := *config
		return &configCopy, nil
	}
	return nil, fmt.Errorf("program %s not found", programName)
}

// UpdateConsumerCount 更新指定程序的消费者数量
func (c *SupervisorManage) UpdateConsumerCount(programName string, targetCount int) error {
	// 1. 获取程序配置
	programConfig, err := c.GetProgramConfig(programName)
	if err != nil {
		return fmt.Errorf("failed to get program config: %w", err)
	}

	// 2. 验证目标数量
	if targetCount < programConfig.MinCount {
		targetCount = programConfig.MinCount
		slog.Info("[supervisor] 目标进程数量调整到最少", "进程名", programName, "最少进程数量", programConfig.MinCount)
	}
	if targetCount > programConfig.MaxCount {
		targetCount = programConfig.MaxCount
		slog.Info("[supervisor] 目标进程数量调整到最多", "进程名", programName, "最多进程数量", programConfig.MaxCount)
	}

	// 获取当前实际运行的进程数
	actualCount, err := c.GetRunningConsumerCount(programName)
	if err != nil {
		slog.Warn("[supervisor] 获取正在运行的进程数量失败", "error", err)
		actualCount = -1
	}

	slog.Info("[supervisor] 当前进程状态",
		"进程名", programName,
		"配置的进程数量", programConfig.CurrentCount,
		"实际运行进程数量", actualCount,
		"目标进程数量", targetCount)

	// 检查是否需要更新
	if programConfig.CurrentCount == targetCount && actualCount == targetCount {
		slog.Info("[supervisor] 不需要更新", "进程名", programName, "当前进程数量", programConfig.CurrentCount)
		return nil
	}

	// 3. 更新配置文件
	if programConfig.CurrentCount != targetCount {
		slog.Info("[supervisor] 更新进程配置文件失败",
			"进程名", programName,
			"当前配置进程数量", programConfig.CurrentCount,
			"目标进程数量", targetCount)

		if err := c.updateNumprocsInConfig(programConfig.ConfigPath, targetCount); err != nil {
			return fmt.Errorf("failed to update config file: %w", err)
		}
	}

	// 4. 使用 supervisorctl 命令重新加载并重启
	slog.Info("[supervisor] 重载进程配置", "进程名", programName)

	// 执行 supervisorctl 命令序列
	commands := []struct {
		args []string
		desc string
	}{
		{[]string{"reread"}, "Reread config"},
		{[]string{"update"}, "Update config"},
		{[]string{"restart", fmt.Sprintf("%s:*", programName)}, "Restart program group"},
	}

	for _, cmdInfo := range commands {
		slog.Info("[supervisor] 执行supervisor命令行控制程序", "动作", cmdInfo.args)
		cmd := exec.Command("supervisorctl", cmdInfo.args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			outputStr := string(output)
			// 对于 reread 和 update，某些情况下返回 "no changes" 不算错误
			if (cmdInfo.desc == "Reread config" || cmdInfo.desc == "Update config") &&
				strings.Contains(outputStr, "no changes") {
				slog.Info("No config changes", "desc", cmdInfo.desc)
				continue
			}
			slog.Error("[supervisor] 命令执行失败",
				"desc", cmdInfo.desc,
				"args", cmdInfo.args,
				"error", err,
				"output", outputStr)
			if cmdInfo.desc == "Restart program group" {
				return fmt.Errorf("failed to restart program: %w, output: %s", err, outputStr)
			}
		} else {
			slog.Info("[supervisor] 命令执行成功", "desc", cmdInfo.desc, "output", string(output))
		}
		time.Sleep(2 * time.Second)
	}

	// 5. 验证进程数量
	if err := c.verifyProcessCountWithRetry(programName, targetCount, 15); err != nil {
		return fmt.Errorf("process count verification failed: %w", err)
	}

	// 6. 更新内存中的配置
	c.programsMu.Lock()
	if config, ok := c.programs[programName]; ok {
		config.CurrentCount = targetCount
	}
	c.programsMu.Unlock()

	slog.Info("[supervisor] 消费者进程数量更新成功", "进程名", programName, "目标数量", targetCount)
	return nil
}

// verifyProcessCountWithRetry 带重试的进程数量验证
func (c *SupervisorManage) verifyProcessCountWithRetry(programName string, expectedCount int, maxRetries int) error {
	for i := 0; i < maxRetries; i++ {
		runningCount, err := c.GetRunningConsumerCount(programName)
		if err != nil {
			slog.Warn("[supervisor]  获取运行中的进程数量失败", "重试", i+1, "error", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if runningCount == expectedCount {
			slog.Info("[supervisor] 进程数量验证成功", "进程名", programName, "运行中的进程数量", runningCount, "重试", i+1)
			return nil
		}

		slog.Info("[supervisor] 等待进程数量达到目标进程数量",
			"进程名", programName,
			"目标进程数量", expectedCount,
			"实际进程数量", runningCount,
			"重试", i+1)

		time.Sleep(3 * time.Second)
	}

	runningCount, _ := c.GetRunningConsumerCount(programName)
	return fmt.Errorf("process count mismatch after %d retries: expected %d, actual %d",
		maxRetries, expectedCount, runningCount)
}

// updateNumprocsInConfig 更新配置文件中的 numprocs 值
func (c *SupervisorManage) updateNumprocsInConfig(configPath string, targetCount int) error {
	content, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	updated := false
	inProgramSection := false

	for i, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		// 检测 program section 开始
		if strings.HasPrefix(trimmedLine, "[program:") && strings.HasSuffix(trimmedLine, "]") {
			inProgramSection = true
			continue
		}

		// 检测 program section 结束
		if inProgramSection && strings.HasPrefix(trimmedLine, "[") && strings.HasSuffix(trimmedLine, "]") {
			inProgramSection = false
			continue
		}

		// 在 program section 内查找 numprocs
		if inProgramSection {
			keyPattern := regexp.MustCompile(`^\s*numprocs\s*=`)
			if keyPattern.MatchString(line) {
				// 替换 numprocs 的值
				newLine := regexp.MustCompile(`(numprocs\s*=\s*)(\d+)`).ReplaceAllString(line, fmt.Sprintf("${1}%d", targetCount))
				lines[i] = newLine
				updated = true
				slog.Debug("[supervisor] 更新进程配置numprocs值成功", "进程配置文件", configPath, "旧值", line, "新值", newLine)
				break
			}
		}
	}

	// 如果没有找到 numprocs，在第一个 program section 后添加
	if !updated && inProgramSection {
		for i, line := range lines {
			if strings.Contains(line, "[program:") {
				// 在 program section 后插入
				newLines := make([]string, 0, len(lines)+1)
				newLines = append(newLines, lines[:i+1]...)
				newLines = append(newLines, fmt.Sprintf("numprocs=%d", targetCount))
				newLines = append(newLines, lines[i+1:]...)
				lines = newLines
				updated = true
				slog.Info("Added missing numprocs to config", "file", configPath, "value", targetCount)
				break
			}
		}
	}

	if !updated {
		return fmt.Errorf("failed to update numprocs in config file: no program section found")
	}

	// 写回文件
	newContent := strings.Join(lines, "\n")
	if err := os.WriteFile(configPath, []byte(newContent), 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	slog.Info("[supervisor] 已经更新进程配置", "进程配置文件", configPath, "numprocs", targetCount)
	return nil
}

// callWithDynamicTimeout 支持动态超时的 RPC 调用
func (c *SupervisorManage) callWithDynamicTimeout(method string, timeout time.Duration, args ...any) (any, error) {
	// 创建带特定超时的临时客户端
	tempClient := &http.Client{Timeout: timeout}

	// 构建 XML-RPC 请求体
	request, err := c.buildXMLRPCRequest(method, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}

	req, err := http.NewRequest("POST", c.config.URL, bytes.NewReader(request))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "text/xml")
	if c.config.Username != "" {
		req.SetBasicAuth(c.config.Username, c.config.Password)
	}

	resp, err := tempClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return c.parseXMLRPCResponse(resp.Body)
}

// waitForProcessesState 等待进程组中的所有进程达到指定状态
func (c *SupervisorManage) waitForProcessesState(groupName string, expectedState int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		processes, err := c.GetAllProcessInfo()
		if err != nil {
			return err
		}

		allInState := true
		for _, proc := range processes {
			if proc.Group == groupName {
				if proc.State != expectedState {
					allInState = false
					slog.Debug("[supervisor] 等待进程组中的进程达到指定状态",
						"进程名", proc.Name,
						"当前状态", proc.State,
						"指定状态", expectedState)
					break
				}
			}
		}

		if allInState {
			slog.Debug("[supervisor] 进程组所有进程已经达到指定状态",
				"进程组", groupName,
				"当前状态", expectedState)
			return nil
		}

		<-ticker.C
	}

	return fmt.Errorf("timeout waiting for processes to reach state %d", expectedState)
}

// stopAllProcessesInGroup 强制停止组中的所有进程
func (c *SupervisorManage) stopAllProcessesInGroup(groupName string) error {
	processes, err := c.GetAllProcessInfo()
	if err != nil {
		return err
	}

	var lastErr error
	for _, proc := range processes {
		if proc.Group == groupName && proc.State == 20 { // RUNNING
			if err := c.stopProcess(proc.Name); err != nil {
				slog.Warn("[supervisor] 关闭进程组失败", "进程组", proc.Name, "error", err)
				lastErr = err
			}
		}
	}

	return lastErr
}

// call 调用 XML-RPC 方法
func (c *SupervisorManage) call(method string, args ...any) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 构建 XML-RPC 请求体
	request, err := c.buildXMLRPCRequest(method, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}

	req, err := http.NewRequest("POST", c.config.URL, bytes.NewReader(request))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "text/xml")
	if c.config.Username != "" {
		req.SetBasicAuth(c.config.Username, c.config.Password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return c.parseXMLRPCResponse(resp.Body)
}

// buildXMLRPCRequest 构建XML-RPC请求
func (c *SupervisorManage) buildXMLRPCRequest(method string, args ...any) ([]byte, error) {
	call := methodCall{
		MethodName: method,
		Params: methodParams{
			Param: make([]methodParam, len(args)),
		},
	}

	for i, arg := range args {
		val := c.buildRPCValue(arg)
		call.Params.Param[i] = methodParam{Value: val}
	}

	return xml.MarshalIndent(call, "", "  ")
}

// buildRPCValue 构建 RPC 值
func (c *SupervisorManage) buildRPCValue(v any) rpcValue {
	result := rpcValue{}

	switch typedVal := v.(type) {
	case int:
		result.Int = &typedVal
	case string:
		result.String = &typedVal
	case bool:
		result.Boolean = &typedVal
	default:
		str := fmt.Sprintf("%v", v)
		result.String = &str
	}

	return result
}

// parseXMLRPCResponse 解析XML-RPC响应
func (c *SupervisorManage) parseXMLRPCResponse(body io.Reader) (any, error) {
	var response methodResponse
	decoder := xml.NewDecoder(body)
	if err := decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// 检查是否有fault
	if response.Fault != nil {
		return nil, c.parseFault(response.Fault)
	}

	// 没有参数返回
	if response.Params == nil || len(response.Params.Param) == 0 {
		return nil, nil
	}

	// 返回第一个参数的值
	return c.extractRPCValue(response.Params.Param[0].Value), nil
}

// parseFault 解析错误信息
func (c *SupervisorManage) parseFault(fault *rpcFault) error {
	if fault == nil {
		return nil
	}

	val := c.extractRPCValue(fault.Value)
	if faultMap, ok := val.(map[string]any); ok {
		if faultCode, ok := faultMap["faultCode"]; ok {
			if faultString, ok := faultMap["faultString"].(string); ok {
				return fmt.Errorf("XML-RPC fault %v: %v", faultCode, faultString)
			}
		}
	}

	return fmt.Errorf("unknown XML-RPC fault: %v", fault)
}

// extractRPCValue 从rpcValue中提取Go值
func (c *SupervisorManage) extractRPCValue(val rpcValue) any {
	if val.Int != nil {
		return *val.Int
	}
	if val.I4 != nil {
		return *val.I4
	}
	if val.String != nil {
		return *val.String
	}
	if val.Boolean != nil {
		return *val.Boolean
	}
	if val.Struct != nil {
		result := make(map[string]any)
		for _, m := range val.Struct.Member {
			result[m.Name] = c.extractRPCValue(m.Value)
		}
		return result
	}
	if val.Array != nil {
		result := make([]any, len(val.Array.Data.Value))
		for i, v := range val.Array.Data.Value {
			result[i] = c.extractRPCValue(v)
		}
		return result
	}
	return nil
}

// GetRunningConsumerCount 获取指定程序当前运行的消费者进程数量
func (c *SupervisorManage) GetRunningConsumerCount(programName string) (int, error) {
	processes, err := c.GetAllProcessInfo()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, p := range processes {
		if p.Group == programName && p.State == 20 { // 20 = RUNNING
			count++
		}
	}

	return count, nil
}

// GetAllRunningConsumerCounts 获取所有程序运行的消费者进程数量
func (c *SupervisorManage) GetAllRunningConsumerCounts() (map[string]int, error) {
	processes, err := c.GetAllProcessInfo()
	if err != nil {
		return nil, err
	}

	counts := make(map[string]int)
	for _, p := range processes {
		if p.State == 20 { // 20 = RUNNING
			counts[p.Group]++
		}
	}

	return counts, nil
}

// GetProcessInfo 获取进程信息
func (c *SupervisorManage) GetProcessInfo(name string) (*ProcessInfo, error) {
	result, err := c.call("supervisor.getProcessInfo", name)
	if err != nil {
		return nil, err
	}
	return c.parseProcessInfo(result), nil
}

// GetAllProcessInfo 获取所有进程信息
func (c *SupervisorManage) GetAllProcessInfo() ([]ProcessInfo, error) {
	result, err := c.call("supervisor.getAllProcessInfo")
	if err != nil {
		return nil, err
	}
	return c.parseProcessList(result), nil
}

// parseProcessInfo 解析进程信息
func (c *SupervisorManage) parseProcessInfo(data any) *ProcessInfo {
	processMap, ok := data.(map[string]any)
	if !ok {
		return &ProcessInfo{Name: "unknown", State: 0}
	}

	info := &ProcessInfo{}

	if name, ok := processMap["name"].(string); ok {
		info.Name = name
	}
	if group, ok := processMap["group"].(string); ok {
		info.Group = group
	}
	if state, ok := processMap["state"].(int); ok {
		info.State = state
	}
	if description, ok := processMap["description"].(string); ok {
		info.Description = description
	}
	if pid, ok := processMap["pid"].(int); ok {
		info.PID = pid
	}
	if exitStatus, ok := processMap["exitstatus"].(int); ok {
		info.ExitStatus = exitStatus
	}

	return info
}

// parseProcessList 解析进程列表
func (c *SupervisorManage) parseProcessList(data any) []ProcessInfo {
	processList, ok := data.([]any)
	if !ok {
		return []ProcessInfo{}
	}

	result := make([]ProcessInfo, 0, len(processList))
	for _, item := range processList {
		if processMap, ok := item.(map[string]any); ok {
			info := ProcessInfo{}

			if name, ok := processMap["name"].(string); ok {
				info.Name = name
			}
			if group, ok := processMap["group"].(string); ok {
				info.Group = group
			}
			if state, ok := processMap["state"].(int); ok {
				info.State = state
			}
			if description, ok := processMap["description"].(string); ok {
				info.Description = description
			}
			if pid, ok := processMap["pid"].(int); ok {
				info.PID = pid
			}
			if exitStatus, ok := processMap["exitstatus"].(int); ok {
				info.ExitStatus = exitStatus
			}

			result = append(result, info)
		}
	}

	return result
}

// GetSupervisorVersion 获取Supervisor版本
func (c *SupervisorManage) GetSupervisorVersion() (string, error) {
	result, err := c.call("supervisor.getSupervisorVersion")
	if err != nil {
		return "", err
	}

	if version, ok := result.(string); ok {
		return version, nil
	}
	return "", nil
}

// GetState 获取Supervisor状态
func (c *SupervisorManage) GetState() (int, string, error) {
	result, err := c.call("supervisor.getState")
	if err != nil {
		return 0, "", err
	}

	stateMap, ok := result.(map[string]any)
	if !ok {
		return 0, "", fmt.Errorf("unexpected response format")
	}

	stateCode := 0
	stateName := ""

	if code, ok := stateMap["statecode"].(int); ok {
		stateCode = code
	}
	if name, ok := stateMap["statename"].(string); ok {
		stateName = name
	}

	return stateCode, stateName, nil
}

// stopProcess 停止单个进程
func (c *SupervisorManage) stopProcess(processName string) error {
	_, err := c.call("supervisor.stopProcess", processName, true)
	return err
}

// StartProcess 启动单个进程
func (c *SupervisorManage) StartProcess(processName string, wait bool) error {
	_, err := c.call("supervisor.startProcess", processName, wait)
	return err
}

// StartProcessGroup 启动进程组
func (c *SupervisorManage) StartProcessGroup(groupName string) error {
	_, err := c.call("supervisor.startProcessGroup", groupName)
	return err
}

// StopProcess 停止单个进程
func (c *SupervisorManage) StopProcess(processName string, wait bool) error {
	_, err := c.call("supervisor.stopProcess", processName, wait)
	return err
}

// StopProcessGroup 停止进程组
func (c *SupervisorManage) StopProcessGroup(groupName string) error {
	_, err := c.call("supervisor.stopProcessGroup", groupName)
	return err
}

// RestartProcess 重启单个进程
func (c *SupervisorManage) RestartProcess(processName string) error {
	// 先停止
	if err := c.StopProcess(processName, true); err != nil {
		return fmt.Errorf("failed to stop process: %w", err)
	}
	// 等待停止
	time.Sleep(2 * time.Second)
	// 再启动
	if err := c.StartProcess(processName, true); err != nil {
		return fmt.Errorf("failed to start process: %w", err)
	}
	return nil
}

// GetProcessesByGroup 获取指定组的所有进程
func (c *SupervisorManage) GetProcessesByGroup(groupName string) ([]ProcessInfo, error) {
	allProcesses, err := c.GetAllProcessInfo()
	if err != nil {
		return nil, err
	}

	var processes []ProcessInfo
	for _, p := range allProcesses {
		if p.Group == groupName {
			processes = append(processes, p)
		}
	}
	return processes, nil
}

// ValidateConfigFile 验证配置文件格式
func (c *SupervisorManage) ValidateConfigFile(configPath string) ([]string, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	inProgramSection := false
	hasCommand := false
	var errors []string

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// 跳过空行和注释
		if trimmed == "" || strings.HasPrefix(trimmed, ";") || strings.HasPrefix(trimmed, "#") {
			continue
		}

		// 检查 program section
		if strings.HasPrefix(trimmed, "[program:") && strings.HasSuffix(trimmed, "]") {
			if inProgramSection && !hasCommand {
				errors = append(errors, fmt.Sprintf("Line %d: missing 'command' in program section", lineNum-1))
			}
			inProgramSection = true
			hasCommand = false
			continue
		}

		// 检查其他 section
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			inProgramSection = false
			continue
		}

		// 只在 program section 内验证
		if !inProgramSection {
			continue
		}

		// 解析 key=value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			errors = append(errors, fmt.Sprintf("Line %d: invalid format (expected key=value)", lineNum))
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "numprocs":
			if num, err := strconv.Atoi(value); err != nil || num < 1 {
				errors = append(errors, fmt.Sprintf("Line %d: invalid numprocs value: %s (must be positive integer)", lineNum, value))
			}
		case "command":
			if value == "" {
				errors = append(errors, fmt.Sprintf("Line %d: command cannot be empty", lineNum))
			} else {
				hasCommand = true
			}
		case "directory":
			if value != "" {
				if _, err := os.Stat(value); os.IsNotExist(err) {
					errors = append(errors, fmt.Sprintf("Line %d: directory does not exist: %s", lineNum, value))
				}
			}
		case "user", "autostart", "autorestart":
			// 可选字段，不强制验证
		}
	}

	// 检查最后一个 program
	if inProgramSection && !hasCommand {
		errors = append(errors, fmt.Sprintf("Line %d: missing 'command' in program section", lineNum))
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading config: %w", err)
	}

	return errors, nil
}

// ValidateAllConfigs 验证所有配置文件
func (c *SupervisorManage) ValidateAllConfigs() (map[string][]string, error) {
	files, err := filepath.Glob(filepath.Join(c.config.ConfigDir, "*.ini"))
	if err != nil {
		return nil, fmt.Errorf("failed to list config files: %w", err)
	}

	results := make(map[string][]string)
	for _, file := range files {
		errors, err := c.ValidateConfigFile(file)
		if err != nil {
			results[filepath.Base(file)] = []string{err.Error()}
		} else if len(errors) > 0 {
			results[filepath.Base(file)] = errors
		}
	}
	return results, nil
}

// UpdateConfig 更新配置（类似 supervisorctl update）
func (c *SupervisorManage) UpdateConfig(programName string) error {
	// 验证配置文件
	configPath := filepath.Join(c.config.ConfigDir, fmt.Sprintf("%s.ini", programName))
	errors, err := c.ValidateConfigFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}
	if len(errors) > 0 {
		return fmt.Errorf("config validation failed:\n%s", strings.Join(errors, "\n"))
	}

	// 刷新配置
	if err := c.RefreshPrograms(); err != nil {
		return fmt.Errorf("failed to refresh programs: %w", err)
	}

	// 使用 supervisorctl 命令重新加载配置
	cmd := exec.Command("supervisorctl", "reread")
	if output, err := cmd.CombinedOutput(); err != nil {
		slog.Warn("[supervisor] 重新加载进程配置失败", "error", err, "输出", string(output))
	} else {
		slog.Info("[supervisor] 重新加载进程配置成功", "输出", string(output))
	}
	time.Sleep(1 * time.Second)

	cmd = exec.Command("supervisorctl", "update")
	if output, err := cmd.CombinedOutput(); err != nil {
		slog.Warn("[supervisor] 更新配置失败", "error", err, "输出", string(output))
	} else {
		slog.Info("[supervisor] 更新配置成功", "输出", string(output))
	}
	time.Sleep(2 * time.Second)

	// 重启程序组
	if err := c.restartProgramGroupWithTimeout(programName, c.config.OpTimeout); err != nil {
		return fmt.Errorf("failed to restart program: %w", err)
	}

	slog.Info("[supervisor] 更新配置成功", "进程名", programName)
	return nil
}

// UpdateAllConfigs 更新所有配置
func (c *SupervisorManage) UpdateAllConfigs() error {
	// 验证所有配置
	results, err := c.ValidateAllConfigs()
	if err != nil {
		return fmt.Errorf("failed to validate configs: %w", err)
	}

	// 检查是否有错误
	if len(results) > 0 {
		for file, errors := range results {
			slog.Error("[supervisor] 进程配置验证失败", "进程配置文件", file, "errors", errors)
		}
		return fmt.Errorf("config validation failed for %d files", len(results))
	}

	// 刷新配置
	if err := c.RefreshPrograms(); err != nil {
		return fmt.Errorf("failed to refresh programs: %w", err)
	}

	// 使用 supervisorctl 命令重新加载配置
	cmd := exec.Command("supervisorctl", "reread")
	if output, err := cmd.CombinedOutput(); err != nil {
		slog.Warn("[supervisor] 重新加载配置失败", "error", err, "输出", string(output))
	} else {
		slog.Info("[supervisor] 重新加载配置成功", "输出", string(output))
	}
	time.Sleep(1 * time.Second)

	cmd = exec.Command("supervisorctl", "update")
	if output, err := cmd.CombinedOutput(); err != nil {
		slog.Warn("[supervisor] 刷新配置失败", "error", err, "输出", string(output))
	} else {
		slog.Info("[supervisor] 刷新配置成功", "输出", string(output))
	}
	time.Sleep(2 * time.Second)

	// 重启所有程序
	programs := c.GetAllPrograms()
	for name := range programs {
		slog.Info("[supervisor] 重启进程", "进程名", name)
		if err := c.restartProgramGroupWithTimeout(name, c.config.OpTimeout); err != nil {
			slog.Error("[supervisor] 重启进程失败", "进程名", name, "error", err)
		}
	}

	slog.Info("[supervisor] 所有配置刷新成功")
	return nil
}

// restartProgramGroupWithTimeout 重启程序组（带超时控制）
func (c *SupervisorManage) restartProgramGroupWithTimeout(groupName string, timeout time.Duration) error {
	slog.Info("[supervisor] 程序进程组", "进程组", groupName, "超时", timeout)

	// 创建带超时的 context
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 在 goroutine 中执行重启操作
	errChan := make(chan error, 1)
	go func() {
		errChan <- c.restartProgramGroup(groupName)
	}()

	// 等待完成或超时
	select {
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("restart failed: %w", err)
		}
		slog.Info("[supervisor] 重启进程组成功", "进程组", groupName)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("restart operation timeout after %v", timeout)
	}
}

// restartProgramGroup 重启程序组
func (c *SupervisorManage) restartProgramGroup(groupName string) error {
	// 使用操作超时时间进行 RPC 调用
	opTimeout := c.config.OpTimeout
	if opTimeout == 0 {
		opTimeout = 120 * time.Second
	}

	// 先尝试停止进程组
	slog.Debug("[supervisor] 正在关闭进程组", "进程组名", groupName)
	_, err := c.callWithDynamicTimeout("supervisor.stopProcessGroup", opTimeout, groupName)
	if err != nil {
		slog.Debug("[supervisor] 关闭进程组失败", "进程组名", groupName, "error", err)
	}

	// 等待进程停止
	stopWaitTime := 5 * time.Second
	slog.Debug("[supervisor] 等待进程停止", "等待时间", stopWaitTime)
	time.Sleep(stopWaitTime)

	// 验证进程是否已停止
	if err := c.waitForProcessesState(groupName, 0, 10*time.Second); err != nil {
		slog.Warn("[supervisor] 进程组没有完全停止", "进程组名", groupName, "error", err)
		if err := c.stopAllProcessesInGroup(groupName); err != nil {
			slog.Warn("[supervisor] 强制停止进程组", "error", err)
		}
	}

	// 启动进程组
	slog.Debug("[supervisor] 启动进程组", "进程组名", groupName)
	_, err = c.callWithDynamicTimeout("supervisor.startProcessGroup", opTimeout, groupName)
	if err != nil {
		return fmt.Errorf("failed to start process group: %w", err)
	}

	// 等待进程启动
	startWaitTime := 5 * time.Second
	slog.Debug("[supervisor] 等待进程组启动", "等待时间", startWaitTime)
	time.Sleep(startWaitTime)

	// 验证进程是否已启动
	if err := c.waitForProcessesState(groupName, 20, 30*time.Second); err != nil {
		slog.Warn("[supervisor] 验证进程组是否完全启动", "进程组名", groupName, "error", err)
	}

	return nil
}
