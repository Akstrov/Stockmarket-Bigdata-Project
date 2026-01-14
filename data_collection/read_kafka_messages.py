from kafka import KafkaConsumer, TopicPartition
import json

BOOTSTRAP_SERVERS = "localhost:29092"
TOPICS = ["stock-data", "reddit-data"]
NUM_MESSAGES = 10  # how many recent messages to peek

consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='latest',   # start at the end of log
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"📡 Peeking last {NUM_MESSAGES} messages per topic...\n")

for topic in TOPICS:
    tp = TopicPartition(topic, 0)  # partition 0
    consumer.assign([tp])
    
    # Get the latest offset
    end_offset = consumer.end_offsets([tp])[tp]
    
    # Calculate where to start
    start_offset = max(0, end_offset - NUM_MESSAGES)
    consumer.seek(tp, start_offset)
    
    print(f"--- Topic: {topic} (showing last {NUM_MESSAGES}) ---")
    count = 0
    for msg in consumer:
        print(msg.value)
        count += 1
        if count >= NUM_MESSAGES:
            break

consumer.close()
print("\n✅ Done peeking messages")
