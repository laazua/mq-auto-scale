"""
消费者 - 简单批量确认版本（最适合多消费者场景）
"""
import pika
import time
import json

class SimpleBatchConsumer:
    def __init__(self, host, port, virtual_host, username, password, queue_name, consumer_id=1):
        credentials = pika.PlainCredentials(username, password)
        self.parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=credentials
        )
        self.queue_name = queue_name
        self.consumer_id = consumer_id
        self.connection = None
        self.channel = None
        
        # 批量确认配置
        self.batch_size = 50  # 累积多少条消息后批量确认
        self.pending_count = 0  # 待确认消息计数
        self.last_delivery_tag = None  # 最后一个消息的tag
        
    def connect(self):
        """建立连接"""
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        # 设置预取计数
        self.channel.basic_qos(prefetch_count=self.batch_size * 2)
        
        # 注册消费回调
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.callback,
            auto_ack=False
        )
        print(f"消费者 {self.consumer_id} 已连接到 RabbitMQ")
        
    def process_message(self, body: bytes) -> bool:
        """处理消息"""
        try:
            message_data = json.loads(body.decode())
            print(f"[{self.consumer_id}] 处理消息 ID: {message_data.get('id')}")
            # 模拟业务处理
            time.sleep(0.01)
            return True
        except Exception as e:
            print(f"[{self.consumer_id}] 处理失败: {e}")
            return False
    
    def callback(self, ch, method, properties, body):
        """消息回调"""
        try:
            # 处理消息
            success = self.process_message(body)
            
            if success:
                # 更新计数和最后一个tag
                self.pending_count += 1
                self.last_delivery_tag = method.delivery_tag
                
                # 达到批量大小，执行批量确认
                if self.pending_count >= self.batch_size:
                    # 批量确认所有累积的消息
                    ch.basic_ack(delivery_tag=self.last_delivery_tag, multiple=True)
                    print(f"[{self.consumer_id}] 批量确认了 {self.pending_count} 条消息 (tag <= {self.last_delivery_tag})")
                    self.pending_count = 0
                    self.last_delivery_tag = None
            else:
                # 处理失败，拒绝单条消息
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                
        except Exception as e:
            print(f"[{self.consumer_id}] 回调错误: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def start(self):
        """开始消费"""
        print(f'消费者 {self.consumer_id} 启动，批量确认大小: {self.batch_size}')
        print('按 CTRL+C 退出\n')
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print(f'\n消费者 {self.consumer_id} 停止中...')
            # 确认最后一批未确认的消息
            if self.pending_count > 0 and self.last_delivery_tag:
                self.channel.basic_ack(delivery_tag=self.last_delivery_tag, multiple=True)
                print(f"最后批量确认了 {self.pending_count} 条消息")
            self.channel.stop_consuming()
            self.connection.close()
            print(f'消费者 {self.consumer_id} 已退出')

# 主程序
if __name__ == "__main__":
    import sys
    
    consumer_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    
    consumer = SimpleBatchConsumer(
        host='192.168.165.88',
        port=5672,
        virtual_host='mq-auto-scale',
        username='admin',
        password='123456',
        queue_name='ama.message.pull',
        consumer_id=consumer_id
    )
    
    consumer.connect()
    consumer.start()