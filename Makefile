
target := mqAutoScale
PYPATH := scripts

build:
	@echo "正在构建 Go 可执行文件..."
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build  -C cmd/mq-auto-scale -mod=readonly -trimpath -ldflags "-s -w" -o ../../$(target)

clean:
	@echo "正在清理构建产物..."
	rm -f $(target)

py-lib:
	@echo "正在安装 Python 依赖库..."
	python3 -m pip install pika -t $(PYPATH)/lib > /dev/null 2>&1

py-producer:
	@echo "正在运行生产者生成数据..."
	export PYTHONPATH=$(PYPATH)/lib &&  python3 -sP $(PYPATH)/producer.py 100000

py-consumer:
	@echo "正在运行消费者消费数据..."
	export PYTHONPATH=$(PYPATH)/lib &&  python3 -sP $(PYPATH)/consumer.py

help:
	@echo "使用以下命令:"
	@echo "  make help         显示此帮助信息"
	@echo "  make build        构建 Go 可执行文件"
	@echo "  make clean        清理构建产物"
	@echo "  make py-lib       安装 Python 依赖库"
	@echo "  make py-producer  运行生产者生成数据"
	@echo "  make py-consumer  运行消费者消费数据"

.PHONY: build clean py-lib py-producer py-consumer help
