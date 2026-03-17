from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def consume_content_events(topic: str, bootstrap_servers: list) -> list:
    """
    Consumes Disney content events from Kafka topic.
    Simulates real-time ingestion of content metadata events.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000
    )

    events = []
    logger.info(f"Consuming from topic: {topic}")

    for message in consumer:
        event = message.value
        events.append(event)
        logger.info(f"Received event: {event.get('content_id')} - {event.get('event_type')}")

    consumer.close()
    logger.info(f"Total events consumed: {len(events)}")
    return events


def get_sample_events() -> list:
    """
    Returns sample Disney content events for local testing
    without a real Kafka cluster.
    """
    return [
        {
            "content_id": "DIS-2024-001",
            "event_type": "content_published",
            "title": "The Lion King Remastered",
            "category": "Movie",
            "region": "US",
            "timestamp": "2024-01-15T10:30:00Z",
            "labor_hours": 120,
            "department": "Post-Production"
        },
        {
            "content_id": "DIS-2024-002",
            "event_type": "content_updated",
            "title": "Mandalorian S4",
            "category": "Series",
            "region": "EU",
            "timestamp": "2024-02-10T08:00:00Z",
            "labor_hours": 85,
            "department": "VFX"
        },
        {
            "content_id": "DIS-2024-003",
            "event_type": "content_published",
            "title": "Encanto 2",
            "category": "Movie",
            "region": "US",
            "timestamp": "2024-03-05T14:00:00Z",
            "labor_hours": 200,
            "department": "Animation"
        },
        {
            "content_id": "DIS-2024-004",
            "event_type": "content_published",
            "title": "National Geographic: Oceans",
            "category": "Documentary",
            "region": "APAC",
            "timestamp": "2024-03-20T09:00:00Z",
            "labor_hours": 60,
            "department": "Editing"
        },
        {
            "content_id": "DIS-2024-005",
            "event_type": "content_updated",
            "title": "Thor: New Era",
            "category": "Movie",
            "region": "US",
            "timestamp": "2024-04-01T11:00:00Z",
            "labor_hours": 45,
            "department": "Post-Production"
        }
    ]


if __name__ == "__main__":
    print("=== Kafka Consumer - Sample Events ===")
    events = get_sample_events()
    for e in events:
        print(f"  [{e['event_type']}] {e['content_id']} - {e['title']}")
