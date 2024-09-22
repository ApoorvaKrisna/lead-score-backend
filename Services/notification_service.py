from flask import Blueprint, request, jsonify, send_from_directory
import pika
import json
notify = Blueprint('leadScore', __name__)

VAPID_PUBLIC_KEY = ""
VAPID_PRIVATE_KEY = ""
VAPID_EMAIL = ""

web_push_subscriptions = []

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='notifications')

@notify.route('/register', methods=['POST'])
def register():
    subscription_info = request.json
    if subscription_info not in web_push_subscriptions:
        web_push_subscriptions.append(subscription_info)
        return jsonify({"message": "Subscription registered successfully."}), 200
    return jsonify({"message": "Subscription already registered."}), 400

@notify.route('/send_notification', methods=['POST'])
def send_notification():
    title = request.json.get('title')
    body = request.json.get('body')

    if not title or not body:
        return jsonify({"message": "Title and body are required."}), 400

    message = json.dumps({"title": title, "body": body, "subscriptions": web_push_subscriptions})
    channel.basic_publish(exchange='', routing_key='notifications', body=message)

    return jsonify({"message": "Notification queued."}), 200

@notify.route('/')
def serve_index():
    return send_from_directory('static', 'index.html')




def send_notification(subscriptions, title, body):
    for subscription in subscriptions:
        try:
            webpush(
                subscription=subscription,
                data=json.dumps({"title": title, "body": body}),
                vapid={
                    "sub": VAPID_EMAIL,
                    "privateKey": VAPID_PRIVATE_KEY,
                    "publicKey": VAPID_PUBLIC_KEY
                }
            )
            print(f"Notification sent to: {subscription['endpoint']}")
        except Exception as e:
            print(f"Failed to send notification: {e}")

def callback(ch, method, properties, body):
    """Callback function to handle messages from the queue."""
    data = json.loads(body)
    title = data['title']
    body = data['body']
    subscriptions = data['subscriptions']

    send_notification(subscriptions, title, body)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='notifications')
channel.basic_consume(queue='notifications', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
