package comm

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	con  *config
	once sync.Once
)

type config struct {
	// 全局配置
	LogLevel                                            string            `yaml:"log_level" default:"info"`
	LogFile                                             string            `yaml:"log_file" defalt:"logs/app.log"`
	LogFormat                                           string            `yaml:"log_format" default:"text"`
	LogAddSource                                        bool              `yaml:"log_add_source" default:"false"`
	LogEnableConsole                                    bool              `yaml:"log_enable_console" default:"false"`
	LogMaxSize                                          int               `yaml:"log_max_size" default:"10"`
	LogMaxBackups                                       int               `yaml:"log_max_backups" default:"7"`
	LogMaxAge                                           int               `yaml:"log_max_age" default:"7"`
	ScheduleInterval                                    time.Duration     `yaml:"schedule_interval" default:"3s"`
	ScheduleQueueProgramMapping                         map[string]string `yaml:"schedule_queue_program"`
	ScheduleScaleUpConfigScaleThreshold                 int               `yaml:"schedule_scaleupconfig_scale_threshold" default:"100000"`
	ScheduleScaleUpConfigConsumerUtilisationThreshold   float64           `yaml:"schedule_scaleupconfig_consumer_utilisation_threshold" default:"0.9"`
	ScheduleScaleUpConfigScaleUpStep                    int               `yaml:"schedule_scaleupconfig_scale_up_step" default:"2"`
	ScheduleScaleUpConfigScaleDownStep                  int               `yaml:"schedule_scaleupconfig_scale_down_step" default:"2"`
	ScheduleScaleDownConfigScaleThreshold               int               `yaml:"schedule_scaledownconfig_scale_threshold" default:"100"`
	ScheduleScaleDownConfigConsumerUtilisationThreshold float64           `yaml:"schedule_scaledownconfig_consumer_utilisation_threshold" default:"0.2"`
	ScheduleScaleDownConfigScaleUpStep                  int               `yaml:"schedule_scaledownconfig_scale_up_step" default:"2"`
	ScheduleScaleDownConfigScaleDownStep                int               `yaml:"schedule_scaledownconfig_scale_down_step" default:"2"`
	ScheduleEnableAutoScaleDown                         bool              `yaml:"schedule_enable_auto_scale_down" default:"true"`
	ScheduleScaleUpCooldown                             time.Duration     `yaml:"schedule_scale_up_cooldown" default:"2m"`
	ScheduleScaleDownCooldown                           time.Duration     `yaml:"schedule_scale_down_cooldown" default:"5m"`

	// MQ 配置
	Mq struct {
		Host     string `yaml:"mq_host" default:"localhost"`
		Port     int    `yaml:"mq_port" default:"15672"`
		Username string `yaml:"mq_user" default:"guest"`
		Password string `yaml:"mq_pass" default:"guest"`
		Vhost    string `yaml:"mq_vhost" default:"/"`
	} `yaml:",inline"`

	// Node Monitor 配置
	NodeMonitor struct {
		CheckInterval         time.Duration `yaml:"node_check_interval" default:"3s"`
		ConsecutiveChecks     int           `yaml:"node_consecutive_checks" default:"3"`
		MaxLoadPerCore        float64       `yaml:"node_max_load_per_core" default:"4.0"`
		MaxCPUUsagePercent    float64       `yaml:"node_max_cpu_usage_percent" default:"80.0"`
		MaxMemoryUsagePercent float64       `yaml:"node_max_memory_usage_percent" default:"70.0"`
	} `yaml:",inline"`

	// Supervisor 配置
	Supervisor struct {
		Url        string        `yaml:"supervisor_url" default:"http://localhost:9001/RPC2"`
		User       string        `yaml:"supervisor_user" default:"admin"`
		Pass       string        `yaml:"supervisor_pass" default:"123456"`
		TimeOut    time.Duration `yaml:"supervisor_timeout" default:"120s"`
		OpTimeOut  time.Duration `yaml:"supervisor_op_timeout" default:"120"`
		ConfigPath string        `yaml:"supervisor_config_path" default:"/etc/supervisord.d/"`
		MinProcess int           `yaml:"supervisor_min_process" default:"1"`
		MaxProcess int           `yaml:"supervisor_max_process" default:"8"`
	} `yaml:",inline"`
}

func LoadConfig() error {
	return initConfig()
}

func initConfig() error {
	fileName := filepath.Join(".", "mq-auto-scale.yaml")
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(bytes, &con)
	if err != nil {
		return err
	}
	return nil
}

func Config() *config {
	if con == nil {
		once.Do(func() {
			if err := initConfig(); err != nil {
				panic(err)
			}
		})
	}
	return con
}
