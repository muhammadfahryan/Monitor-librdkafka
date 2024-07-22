from confluent_kafka import Producer
from prometheus_client import start_http_server, Gauge
import json
import time

# Konfigurasi Kafka Producer
bootstrap_servers = 'localhost:9093localhost:9093,localhost:9093'
conf = {
    'bootstrap.servers': bootstrap_servers,
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': 'D:/ca.crt',
    'sasl.username': 'username',
    'sasl.password': 'password',
    'statistics.interval.ms': 10000,
    
}

# Callback untuk menangani statistik dari Kafka
def stats_cb(stats_json_str):
    stats = json.loads(stats_json_str)
    
    # get latency in object "broker"
    if 'brokers' in stats:
        for broker_id, broker in stats['brokers'].items():
            if 'int_latency' in broker:
                int_latency_avg.set(broker['int_latency']['avg'])
                print(f'Internal latency avg for {broker_id}: {broker["int_latency"]["avg"]}')
            if 'outbuf_latency' in broker:
                outbuf_latency_avg.set(broker['outbuf_latency']['avg'])
                print(f'Outbuf latency avg for {broker_id}: {broker["outbuf_latency"]["avg"]}')
            if 'rtt' in broker:
                rtt_avg.set(broker['rtt']['avg'])
                print(f'RTT avg for {broker_id}: {broker["rtt"]["avg"]}')
            if 'throttle' in broker:
                print('Throttle data: {}'.format(broker['throttle']))
                throttle_time.set(0)
                print('Throttle time: 0')    
            if 'throttle' in broker:
                throttle_time.set(broker['throttle']['avg'])
                print(f'Throttle time avg for {broker_id}: {broker["throttle"]["avg"]}')
    
    # get batch metrics in object "topics"
    if 'topics' in stats:
        for topic_name, topic_stats in stats['topics'].items():
            if 'batchsize' in topic_stats:
                batch_size_avg.set(topic_stats['batchsize']['avg'])
                print(f'Batch size avg for topic {topic_name}: {topic_stats["batchsize"]["avg"]}')
            if 'batchcnt' in topic_stats:
                batch_count_avg.set(topic_stats['batchcnt']['avg'])
                print(f'Batch count avg for topic {topic_name}: {topic_stats["batchcnt"]["avg"]}')


# Inisialisasi Kafka Producer dengan callback statistik
conf['stats_cb'] = stats_cb
producer = Producer(**conf)

# Prometheus Gauges untuk metrik
int_latency_avg = Gauge('kafka_int_latency_avg', 'Average internal producer queue latency in microseconds')
outbuf_latency_avg = Gauge('kafka_outbuf_latency_avg', 'Average internal request queue latency in microseconds')
rtt_avg = Gauge('kafka_rtt_avg', 'Average round-trip time in microseconds')
throttle_time = Gauge('kafka_throttle_time', 'Throttle time in milliseconds')
batch_size_avg = Gauge('kafka_batch_size_avg', 'Average batch size in bytes')
batch_count_avg = Gauge('kafka_batch_count_avg', 'Average number of messages per batch')

# Mulai server Prometheus di port 8000
start_http_server(8000)

# Fungsi untuk mengirim pesan ke Kafka
def produce_messages():
    producer.produce('ryan.test.metric', key='key', value='message')
    producer.poll(0)

# Loop untuk mengirim pesan setiap 5 detik
while True:
    produce_messages()
    time.sleep(5)
