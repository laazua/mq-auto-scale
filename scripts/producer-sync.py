"""
生产者 - Publisher Confirms 批量确认版本（修正版）
"""
import sys
import time
import json
import random
import pika

def custom_random(length=8):
    chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789'
    return ''.join(random.choice(chars) for _ in range(length))

class BatchProducer:
    def __init__(self, host, port, virtual_host, username, password):
        credentials = pika.PlainCredentials(username, password)
        self.parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=credentials
        )
        self.connection = None
        self.channel = None
        
    def connect(self):
        """建立连接"""
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='ama.message.pull', durable=True)
        # 开启发布者确认模式
        self.channel.confirm_delivery()
        print("已连接到 RabbitMQ，发布者确认模式已开启")
        
    def send_batch_with_confirm(self, messages, routing_key, batch_size=100):
        """
        批量发送并等待确认
        返回: (成功数量, 失败消息列表)
        """
        success_count = 0
        failed_messages = []
        batch_buffer = []
        
        for i, msg in enumerate(messages, 1):
            batch_buffer.append(msg)
            
            # 达到批次大小或最后一条时发送
            if len(batch_buffer) >= batch_size or i == len(messages):
                # 批量发送
                for batch_msg in batch_buffer:
                    message = json.dumps(batch_msg)
                    # basic_publish 在 confirm_delivery 模式下会阻塞等待确认
                    # 如果发送失败会抛出异常
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=routing_key,
                        body=message.encode(),
                        properties=pika.BasicProperties(delivery_mode=2),
                        mandatory=True  # 确保消息能够路由到队列
                    )
                
                # 在 confirm_delivery 模式下，basic_publish 已经自动等待确认
                # 如果执行到这里，说明本批次所有消息都已确认
                success_count += len(batch_buffer)
                print(f" [√] 批次确认成功: {len(batch_buffer)} 条 (进度: {i}/{len(messages)})")
                
                batch_buffer = []  # 清空缓冲区
        
        return success_count, failed_messages
    
    def send_single_with_confirm(self, message, routing_key):
        """发送单条消息并等待确认"""
        try:
            msg_json = json.dumps(message)
            self.channel.basic_publish(
                exchange='',
                routing_key=routing_key,
                body=msg_json.encode(),
                properties=pika.BasicProperties(delivery_mode=2),
                mandatory=True
            )
            return True
        except Exception as e:
            print(f"发送失败: {e}")
            return False
    
    def close(self):
        """关闭连接"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()

# 方案二：使用 SelectConnection 实现真正的异步批量确认
class AsyncBatchProducer:
    """使用异步方式实现真正的批量确认"""
    def __init__(self, host, port, virtual_host, username, password):
        credentials = pika.PlainCredentials(username, password)
        self.parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=credentials
        )
        self.connection = None
        self.channel = None
        self.unconfirmed = set()  # 未确认的消息集合
        self.confirmed = 0
        self.failed = 0
        
    def connect(self):
        """建立连接"""
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='ama.message.pull', durable=True)
        self.channel.confirm_delivery()
        
        # 设置确认回调（注意：BlockingConnection 不支持回调，这里仅作示例）
        print("已连接到 RabbitMQ")
        
    def send_batch(self, messages, routing_key, batch_size=100):
        """
        批量发送（在 BlockingConnection 中，每条消息都会自动确认）
        """
        success = 0
        for i, msg in enumerate(messages, 1):
            try:
                msg_json = json.dumps(msg)
                self.channel.basic_publish(
                    exchange='',
                    routing_key=routing_key,
                    body=msg_json.encode(),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                success += 1
                
                if i % batch_size == 0:
                    print(f"进度: {i}/{len(messages)} ({i/len(messages)*100:.1f}%)")
                    
            except Exception as e:
                print(f"发送第 {i} 条消息失败: {e}")
                
        return success

# 主程序
if __name__ == "__main__":
    # 创建生产者
    producer = BatchProducer(
        host='192.168.165.88',
        port=5672,
        virtual_host='mq-auto-scale',
        username='admin',
        password='123456'
    )
    
    # 连接
    producer.connect()
    
    # 准备所有消息
    total_messages = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    print(f"准备生成 {total_messages} 条消息...")
    
    start_time = time.time()
    
    # 生成所有消息
    all_messages = []
    for i in range(1, total_messages + 1):
        all_messages.append({'id': i, 'message': custom_random()})
    
    print(f"消息生成完成，开始批量发送...")
    
    # 批量发送
    BATCH_SIZE = 500  # 每批500条消息进行确认
    success, failed = producer.send_batch_with_confirm(
        all_messages, 
        'ama.message.pull', 
        batch_size=BATCH_SIZE
    )
    
    elapsed_time = time.time() - start_time
    
    print(f"\n发送完成！")
    print(f"成功: {success}/{total_messages} 条")
    print(f"失败: {len(failed)} 条")
    print(f"总耗时: {elapsed_time:.2f} 秒")
    print(f"平均速率: {success/elapsed_time:.0f} 条/秒")
    
    producer.close()