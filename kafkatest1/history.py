import json
from kafka import KafkaConsumer
#Hello
KAFKA_BROKER = "localhost:9092"
TOPIC_VALIDATED = "validated_orders"

consumer = KafkaConsumer(
    TOPIC_VALIDATED,
    bootstrap_servers=KAFKA_BROKER
)

customer_orders = {}

for message in consumer:
    try:
        order = json.loads(message.value.decode("utf-8"))
        if not all(k in order for k in ["customer", "book", "status", "payment_option"]):
            continue

        customer = order["customer"]
        customer_orders.setdefault(customer, []).append(order)

        print(f"\nOrder history for {customer}:")
        for past_order in customer_orders[customer]:
            print(f"Book Title: {past_order['book']} | Status: {past_order['status']} | Payment: {past_order['payment_option']}")

    except Exception as e:
        print(f"Error in history consumer: {e}")
