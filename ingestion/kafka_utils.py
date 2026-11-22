import json
import config

try:
    from confluent_kafka import Producer as ConfluentProducer

    _producer = ConfluentProducer({"bootstrap.servers": config.KAFKA_BOOTSTRAP})

    def send(topic, key, payload):
        val = json.dumps(payload).encode("utf-8")
        k = str(key).encode("utf-8")
        while True:
            try:
                _producer.produce(topic, key=k, value=val)
                break
            except BufferError:
                _producer.poll(0.5)
        _producer.poll(0)

    def flush():
        _producer.flush()

except ImportError:
    from kafka import KafkaProducer as PythonKafkaProducer

    _producer = PythonKafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=10000
    )

    def send(topic, key, payload):
        _producer.send(topic, key=key, value=payload)

    def flush():
        _producer.flush()
