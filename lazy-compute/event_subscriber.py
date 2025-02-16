# this code will read the `on_msg_unsent` event and then process it
# in this case, the process will be to update the unread message counter value
# Now we need to test this whole system properly
# TODO: 
# 1. Write unit tests for `flush_buffer`, `update_unread_msg_count`, and `on_message` functions.
# 2. Write integration tests to ensure the entire message processing flow works correctly.
# 3. Test the system under load to ensure it can handle a high volume of messages.
# 4. Consider how to scale the Redis instance and RabbitMQ consumers to handle increased load.
# 5. Ensure the buffer flushing mechanism works efficiently under different load conditions.


import aio_pika
import json
import redis
from typing import List, Dict
import asyncio
import aiohttp
from collections import defaultdict

BASE_URL = "http://127.0.0.1:8000"
NUM_QUEUES = 5

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
# Buffer to store messages
message_buffer: defaultdict[int, List[int]] = defaultdict(list)


async def flush_buffer():
    while True:
        await asyncio.sleep(10)  # Flush every 10 seconds
        print(f" [x] Flushing buffer...")
        keys_to_delete = []
        for user_id, sender_ids in message_buffer.items():
            if sender_ids:
                print(f" [x] Updating unread message count for user_id: {user_id} with sender_ids: {sender_ids}")
                await update_unread_msg_count(user_id, sender_ids)
                keys_to_delete.append(user_id) 

        # delete the keys from the buffer
        for user_id in keys_to_delete:
            del message_buffer[user_id]

async def update_unread_msg_count(user_id: int, sender_ids: List[int]):
    url = f"{BASE_URL}/users/update_unread_msg_count"
    data = {"new_sender_ids": sender_ids}

    # async version
    async with aiohttp.ClientSession() as session:
        async with session.post(url, params={"user_id":user_id}, json=data) as response:
            print(f" [x] Response: {await response.json()}")
            return await response.json()

async def on_message(message: aio_pika.IncomingMessage, queue_name: str):
    async with message.process():
        message_body = json.loads(message.body)
        user_id = message_body['to']
        print(f" [x] Received {message_body} from queue {queue_name} for user_id: {user_id}")
        sender_id = message_body['from']
        message_buffer[user_id].append(sender_id)
        print(f" [x] Buffer: {message_buffer}")


async def main():
    # Start the buffer flushing task
    asyncio.create_task(flush_buffer())
    
    # Setup RabbitMQ consumer
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=1)

    exchange = await channel.declare_exchange('whatsapp', aio_pika.ExchangeType.DIRECT)

    for i in range(NUM_QUEUES):
        queue_name = f'chats_{i}'
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key=str(i))
        await queue.consume(lambda message: on_message(message=message, queue_name=queue_name))

    print(f" [*] Waiting for messages. To exit press CTRL+C")

    await asyncio.Future()  # Keep the event loop running indefinitely


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"An error occurred: {e}")