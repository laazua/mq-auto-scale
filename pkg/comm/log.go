package comm

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// Config 日志配置
type logConfig struct {
	// 日志文件路径
	LogFilePath string
	// 日志级别: debug, info, warn, error
	Level string
	// 输出格式: json, text
	Format    string
	AddSource bool
	// 是否输出到控制台
	EnableConsole bool
	// 日志切割配置
	MaxSize    int  // 每个日志文件保存的最大尺寸 单位：M
	MaxBackups int  // 保留旧文件的最大个数
	MaxAge     int  // 保留旧文件的最大天数
	Compress   bool // 是否压缩/归档旧文件
}

// Logger 日志实例
type Logger struct {
	*slog.Logger
}

// NewLogger 创建新的日志实例
func newLogger(config logConfig) (*Logger, error) {
	// 解析日志级别
	var level slog.Level
	switch config.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	// 创建 lumberjack 日志滚动器
	lumberjackLogger := &lumberjack.Logger{
		Filename:   config.LogFilePath,
		MaxSize:    config.MaxSize,
		MaxBackups: config.MaxBackups,
		MaxAge:     config.MaxAge,
		Compress:   config.Compress,
	}

	// 创建日志输出目标
	var writers []io.Writer

	// 添加文件输出
	writers = append(writers, lumberjackLogger)

	// 添加控制台输出
	if config.EnableConsole {
		writers = append(writers, os.Stdout)
	}

	// 创建多路写入器
	multiWriter := io.MultiWriter(writers...)

	// 创建 Handler 选项
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: config.AddSource,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// 自定义时间格式
			if a.Key == slog.TimeKey {
				if t, ok := a.Value.Any().(time.Time); ok {
					a.Value = slog.StringValue(t.Format("2006-01-02 15:04:05.000"))
				}
			}
			return a
		},
	}

	// 根据配置选择输出格式
	var handler slog.Handler
	if config.Format == "json" {
		handler = slog.NewJSONHandler(multiWriter, opts)
	} else {
		handler = slog.NewTextHandler(multiWriter, opts)
	}

	return &Logger{
		Logger: slog.New(handler),
	}, nil
}

// WithFields 添加结构化字段
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	var attrs []any
	for k, v := range fields {
		attrs = append(attrs, slog.Any(k, v))
	}
	return &Logger{
		Logger: l.Logger.With(attrs...),
	}
}

// WithError 添加错误字段
func (l *Logger) WithError(err error) *Logger {
	return &Logger{
		Logger: l.Logger.With(slog.String("error", err.Error())),
	}
}

// Sync 同步日志（用于程序退出前）
func (l *Logger) Sync() error {
	// slog 没有直接的 Sync 方法，但 lumberjack 实现了 Sync
	// 这里返回 nil 因为 slog 会自动处理
	return nil
}

// 辅助函数：获取调用者信息
func getCallerInfo() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	return filepath.Base(file) + ":" + string(rune(line))
}

func SetupLog() error {
	cfg := logConfig{
		LogFilePath:   Config().LogFile,
		Format:        Config().LogFormat,
		Level:         Config().LogLevel,
		AddSource:     Config().LogAddSource,
		EnableConsole: Config().LogEnableConsole,
		MaxSize:       Config().LogMaxSize,
		MaxBackups:    Config().LogMaxBackups,
		MaxAge:        Config().LogMaxAge,
	}
	logger, err := newLogger(cfg)
	if err != nil {
		return nil
	}
	slog.SetDefault(logger.Logger)
	return nil
}
