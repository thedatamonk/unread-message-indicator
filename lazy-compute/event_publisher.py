"""
# this is the publisher that will publish the `on_msg_unsent` event to rabbitmq
# Event structure
    {
        src = 'A',
        dest = 'B',
        msg = '.....'
    }
"""
import pika
import json
import time
import random

NUM_QUEUES = 5

# build connection, channels, exchanges and queues
# also bind the queue to the exchange
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


# fanout exchange broadcasts the messages it receives to all the queues it knows
channel.exchange_declare(exchange='whatsapp', exchange_type='direct')


# utility function to generate events at random timestamps

def publish_msg_unsent_event(message):
    routing_key = str(message['to'] % NUM_QUEUES)
    channel.basic_publish(exchange='whatsapp', routing_key=routing_key, body=json.dumps(message))
    print (f" [x] Sent {message}")



if __name__ == "__main__":
    for i in range(300):
        time.sleep(1)
        from_, to_ = random.sample(range(1, 100), 2)
        
        # basically this message was not delivered due to recipient being offline
        # so in this case, an `on_msg_unsent` event will be sent to the chats queue
        message = {
            "event": 'on_msg_unsent',
            "from": from_,
            "to": to_
        }

        publish_msg_unsent_event(message)





