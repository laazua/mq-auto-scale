### mq-auto-scale

* **说明**
  - 根据队列中的消息数据进行消费者数量自动扩缩
  - 扩缩容的队列都在同一个虚拟机下

* **请求接口**
```bash
# curl -u 'admin:123456' 'http://192.168.165.88:15672/api/queues/<vhost>/<qname>'
curl -u 'admin:123456' 'http://192.168.165.88:15672/api/queues/mq-auto-scale/ama.message.pull'
```