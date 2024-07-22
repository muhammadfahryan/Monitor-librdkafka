# Monitor-librdkafka

Dalam postingan ini, saya akan menunjukkan kepada Anda bagaimana kita dapat mengekspos metrik konsumen librdkafka sehingga dapat dibaca oleh Prometheus.

## Pendahuluan

Librdkafka adalah pustaka Kafka client yang ditulis dalam C dan memiliki binding untuk berbagai bahasa pemrograman, termasuk Python. Dalam proyek ini, kita akan memanfaatkan librdkafka untuk memantau metrik konsumen Kafka dan mengekspornya ke Prometheus untuk pemantauan yang lebih baik.

## Persiapan

Pastikan Anda sudah menginstal dependensi berikut:

- confluent_kafka
- prometheus_client

Anda dapat menginstal dependensi ini menggunakan pip:

```bash
pip install confluent_kafka prometheus_client
```

## Code Python

Dalam proyek ini, saya menggunakan kode Python yang mengambil nilai latency dan metrik lainnya dari librdkafka. Jika kita lihat pada dokumentasi [librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_INTRODUCTION.html#consumer-groups), kita bisa mendapatkan berbagai metrik dari statistik yang disediakan.

## Memeriksa Metrik di Prometheus

Setelah menjalankan script Python untuk produser atau konsumer Kafka, server Prometheus akan mulai mengekspor metrik di localhost:8000 dan dapat memeriksa metrik dengan mengunjungi URL berikut di browser :

```
http://localhost:8000
```

![image](https://github.com/user-attachments/assets/30d4328b-e42a-4ce0-a379-5ab3a888161d)
