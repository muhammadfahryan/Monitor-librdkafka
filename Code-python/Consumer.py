from confluent_kafka import Consumer, KafkaException, KafkaError
from prometheus_client import start_http_server, Gauge
import json
import time

# Konfigurasi Kafka Consumer
bootstrap_servers = 'localhost:9093,localhost:9093,localhost:9093'
conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'example-group',
    'auto.offset.reset': 'earliest',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': 'D:/ca.crt',
    'sasl.username': 'username',
    'sasl.password': 'password',
    'enable.auto.commit': False,
    'statistics.interval.ms': 10000,
}

# Callback untuk menangani statistik dari Kafka
def stats_cb(stats_json_str):
    stats = json.loads(stats_json_str)
    
    if 'brokers' in stats:
        for broker_id, broker in stats['brokers'].items():
            if 'int_latency' in broker:
                int_latency_avg.labels(broker_id).set(broker['int_latency']['avg'])
                print(f'Internal latency avg for {broker_id}: {broker["int_latency"]["avg"]}')
            if 'outbuf_latency' in broker:
                outbuf_latency_avg.labels(broker_id).set(broker['outbuf_latency']['avg'])
                print(f'Outbuf latency avg for {broker_id}: {broker["outbuf_latency"]["avg"]}')
            if 'rtt' in broker:
                rtt_avg.labels(broker_id).set(broker['rtt']['avg'])
                print(f'RTT avg for {broker_id}: {broker["rtt"]["avg"]}')
            if 'throttle' in broker:
                throttle_time.labels(broker_id).set(broker['throttle']['avg'])
                print(f'Throttle time avg for {broker_id}: {broker["throttle"]["avg"]}')
    
    if 'topics' in stats:
        for topic_name, topic_stats in stats['topics'].items():
            if 'batchsize' in topic_stats:
                batch_size_avg.labels(topic_name).set(topic_stats['batchsize']['avg'])
                print(f'Batch size avg for topic {topic_name}: {topic_stats["batchsize"]["avg"]}')
            if 'batchcnt' in topic_stats:
                batch_count_avg.labels(topic_name).set(topic_stats['batchcnt']['avg'])
                print(f'Batch count avg for topic {topic_name}: {topic_stats["batchcnt"]["avg"]}')
            if 'consumer_lag' in topic_stats:
                consumer_lag_avg.labels(topic_name).set(topic_stats['consumer_lag']['avg'])
                print(f'Consumer lag avg for topic {topic_name}: {topic_stats["consumer_lag"]["avg"]}')

# Inisialisasi Kafka Consumer dengan callback statistik
conf['stats_cb'] = stats_cb
consumer = Consumer(**conf)

# Subscribe to the topic
consumer.subscribe(['elastic-schema'])

# Prometheus Gauges untuk metrik
int_latency_avg = Gauge('kafka_int_latency_avg', 'Average internal producer queue latency in microseconds', labelnames=['broker'])
outbuf_latency_avg = Gauge('kafka_outbuf_latency_avg', 'Average internal request queue latency in microseconds', labelnames=['broker'])
rtt_avg = Gauge('kafka_rtt_avg', 'Average round-trip time in microseconds', labelnames=['broker'])
throttle_time = Gauge('kafka_throttle_time', 'Throttle time in milliseconds', labelnames=['broker'])
consumer_lag_avg = Gauge('kafka_consumer_lag_avg', 'Average consumer lag in messages', labelnames=['topic'])
batch_size_avg = Gauge('kafka_batch_size_avg', 'Average batch size in bytes', labelnames=['topic'])
batch_count_avg = Gauge('kafka_batch_count_avg', 'Average number of messages per batch', labelnames=['topic'])

# Mulai server Prometheus di port 8000
start_http_server(8000)

# Fungsi untuk mengkonsumsi pesan dari Kafka
def consume_messages():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Pesan diterima
                print('Received message: {}'.format(msg.value().decode('utf-8')))
                consumer.commit(msg)
    except KeyboardInterrupt:
        pass
    finally:
        # Tutup konsumer dengan benar
        consumer.close()

# Loop untuk mengkonsumsi pesan terus-menerus
consume_messages()
