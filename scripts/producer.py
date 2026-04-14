"""
生产者 - 简单批量提交版本（最高性能）
"""
import sys
import time
import json
import random
import pika

def custom_random(length=8):
    chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789'
    return ''.join(random.choice(chars) for _ in range(length))

# 配置连接
credentials = pika.PlainCredentials('admin', '123456')
parameters = pika.ConnectionParameters(
    host='192.168.165.88',
    port=5672,
    virtual_host='mq-auto-scale',
    credentials=credentials
)

# 建立连接
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='ama.message.pull', durable=True)

# 批量发送参数
BATCH_SIZE = 1000  # 每批发送1000条
total_messages = int(sys.argv[1]) if len(sys.argv) > 1 else 10000

print(f"开始发送 {total_messages} 条消息，批次大小: {BATCH_SIZE}")
start_time = time.time()

batch_buffer = []
sent_count = 0

for i in range(1, total_messages + 1):
    # 准备消息
    message_data = {'id': i, 'message': custom_random()}
    batch_buffer.append(json.dumps(message_data))
    
    # 达到批次大小或最后一批时发送
    if len(batch_buffer) >= BATCH_SIZE or i == total_messages:
        # 批量发送
        for msg in batch_buffer:
            channel.basic_publish(
                exchange='',
                routing_key='ama.message.pull',
                body=msg.encode(),
                properties=pika.BasicProperties(delivery_mode=2)
            )
        
        sent_count += len(batch_buffer)
        print(f"已发送: {sent_count}/{total_messages} ({sent_count/total_messages*100:.1f}%)")
        batch_buffer = []  # 清空缓冲区

elapsed_time = time.time() - start_time

print(f"\n发送完成！")
print(f"总耗时: {elapsed_time:.2f} 秒")
print(f"平均速率: {total_messages/elapsed_time:.0f} 条/秒")

connection.close()