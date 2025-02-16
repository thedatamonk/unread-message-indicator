"""
Approach #2

1. We would need a pub/sub system that would subscribe to `on_message_unsent` event.
    - we would need a publisher of this event (rabbitmq)
    - consumers would be the workers that would update the # of unread messages for each user
    - and then this updated number needs to be periodically written to some DB.
    - since our access pattern is always via a key (for a given user, get the # of unique users from whom there are unread messages)
       we can use Redis.

Tech required:
1. rabbitmq
    - so rabbitmq will help us develop a pub-sub system that will publish 'on_message_unsent' event
    - the `unread_message_counter` workers will subscribe to this event and consume it
    - after consuming they will update the redis DB with the updated count for the user.
    - we also have to make sure the queues are partitioned by receiver_id
2. fastAPI

3. redis


List of APIs - 

**Read path**
1. get_status
2. clear_status

**Write path**
1. update the status

"""

### We can define the APIs here ###

"""
We need 2 APIs to get status and reset status.
We don't need an API to update the status; the consumer workers can directly write to Redis DB.
    Not sure if that's a bad idea though. We will see this later.
"""


import redis
from fastapi import FastAPI
from typing import List
from pydantic import BaseModel

app = FastAPI()
# create redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

class UpdateUnreadMsgCountRequest(BaseModel):
    new_sender_ids: List[int]

@app.post('/users/clear_unread_msg_count')    
def clear_status(user_id: int):
    result = redis_client.delete(user_id)
    if result:
        return {"detail": f" [x] - [User: {user_id}] Unread message count reset successful."}
    else:
        return {"detail": f" [x] - No user with `user_id` {user_id} found in DB."}


@app.get("/users/get_unread_msg_count")
def get_status(user_id: int):
    length = redis_client.scard(user_id)
    if length > 0:       
        print (f" [x] - # of unread messages for user_id: {user_id} are {length}.")
        return {user_id: length}
    else:
        print (f" [x] - No unread messages found for user_id: {user_id}")
        return {user_id: 0}

@app.post("/users/update_unread_msg_count")
def update_status(user_id: int, request: UpdateUnreadMsgCountRequest):
    """
    Update the unread message count for a user.

    Parameters:
    user_id (int): The ID of the user.
    request (UpdateUnreadMsgCountRequest): A request body containing a list of sender IDs whose messages are unread.
    """
    new_sender_ids = request.new_sender_ids
    if isinstance(new_sender_ids, list):
        redis_client.sadd(user_id, *new_sender_ids)
        return {"detail": f" [x] - [User: {user_id}] Unread message count update successful."}
    else:
        return {"detail": f" [x] - `new_sender_ids` must be List type, found {type(new_sender_ids)} instead."}



