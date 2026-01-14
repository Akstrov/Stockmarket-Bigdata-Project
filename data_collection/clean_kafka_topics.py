from kafka.admin import KafkaAdminClient, NewTopic

# ----------------------------------
# Config
# ----------------------------------
BOOTSTRAP_SERVERS = "localhost:29092"
TOPICS = ["stock-data", "reddit-data"]

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

for topic in TOPICS:
    print(f"⚠️ Purging topic: {topic}")
    
    # Option 1: delete + recreate topic
    try:
        admin.delete_topics([topic])
        print(f"   ✅ Topic deleted: {topic}")
    except Exception as e:
        print(f"   ⚠️ Could not delete {topic}: {e}")

admin.close()
print("\n✅ All topics purged!")
