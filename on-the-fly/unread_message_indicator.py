"""
Develop a system that can count the unique users from which the reciever has unread messages

For instance:
A -> B: 2 messages
C -> B: 10 messages
D -> B: 7 messages

B -> A: 1 message
B -> D: 5 messages

So on A's mobile, it should show '1' on the messaging app's icon
So on B's mobile, it should show '3' on the messaging app's icon
So on D's mobile, it should show '1' on the messaging app's icon


Also once the user clicks on the messaging app's icon, the count should become 0, and nothing should be displayed.


Don't design the UI. That's not the purpose of this demo.
But design a system that will perform the following - 
1. computes count of unique 'unread' messages.
2. resets this count to 0, once the user opens the app.

**Approach 1: Count on the fly*
As soon as the user receives a message from a new sender, then update the unread_message_indicator


Tech we will require - 
1. MySQL DB
2. FastAPI

**MySQL DB**
List of tables:
1. user
    - id
    - name
2. messages
    - id
    - msg
    - from (fk)
    - to (fk)
    - timestamp
3. user_activity: stores the app "login" time of each user
    - user_id
    - last_read_at

**APIs**
1. GET `get_unread_msg_count` -> we only need this
2. POST `update_unread_msg_count` -> we dont need this since we are computing it on the fly and no need to store it anywhere
3. POST `reset_unread_msg_count` -> we dont even need this since there is nothing to reset to. What we can do is detect when the user opens the app then we should not render the count.

    
    Let's play with the SQL queries that each of the APIs will eventually call and try to optimise them.
    
    Then we can wrap them around an API.


"""
from fastapi import FastAPI, HTTPException
from .db import get_connection
from faker import Faker
import random
from datetime import datetime


# Initialise FastAPI app
app = FastAPI()
faker_obj = Faker()

def get_valid_user_ids():
    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM users")
            return [row['id'] for row in cursor.fetchall()] # get the ids for each valid user
    except Exception as e:
        print (f"`get_valid_user_ids` failed with the following error: {e}")
    finally:
        conn.close()


def insert_random_messages(num_msgs, user_ids):
    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            query = """
            INSERT INTO messages (`created_at`, `msg`, `from`, `to`)
            VALUES (%s, %s, %s, %s)
            """
            records = []
            for _ in range(num_msgs):
                sender = random.choice(user_ids)
                recipient = random.choice([u for u in user_ids if u != sender])
                message = f"Random message from user {sender}."
                created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                records.append((created_at, message, sender, recipient))
            
            cursor.executemany(query, records)
            conn.commit()
    finally:
        conn.close()


def update_last_login_ts_for_users(user_ids):
    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            values = ", ".join(["(%s, %s)"] * len(user_ids))
            records = []
            for user_id in user_ids:
                records.append(user_id)
                records.append(now)

            # UPSERT query
            query = f"""
            INSERT INTO user_activity (user_id, last_read_at)
            VALUES {values}
            ON DUPLICATE KEY UPDATE last_read_at = VALUES(last_read_at)
            """

            cursor.execute(query, records)
            conn.commit()
    finally:
        conn.close()

# Get API to fetch unique unread receipeints
@app.get("/messages/{to_user}")
def get_unread_msg_count(to_user: int):
    try:
        # connect to the messaging app database
        connection = get_connection()

        # begin a transaction
        connection.begin()

        with connection.cursor() as cursor:
            # get last_read_at for the user
            get_last_read_at = "SELECT last_read_at from user_activity where user_id = %s"
            cursor.execute(get_last_read_at, (to_user, ))
            result = cursor.fetchone()

            if not result or not result['last_read_at']:
                return {"detail": f"No `last_read_at` found for user with user_id = {to_user}"}
                # raise HTTPException(status_code=404, detail=f"No `last_read_at` found for user with user_id={to_user}")
            
            # extract the last_read_at value
            last_read_at_ts = result['last_read_at']
            get_unique_unread_msgs = """
            SELECT COUNT(DISTINCT `from`) AS unread_count 
            FROM messages 
            WHERE `to` = %s AND created_at > %s
            """

            cursor.execute(get_unique_unread_msgs, (to_user, last_read_at_ts))
            unread_msg_query_result = cursor.fetchone()

            if not unread_msg_query_result:
                return {"unread_count": 0}

            # commit the transaction if everything succeeds
            connection.commit()
            return {"unread_count": unread_msg_query_result['unread_count']}
    except Exception as e:
        # rollback the transaction in case of an error
        connection.rollback()
    
    finally:
        # close the database connection
        connection.close()

@app.post("/users/create")
def create_users(num_users: int):
    conn = get_connection()

    user_names = []
    for i in range(num_users):
        user_names.append((faker_obj.name(),))

    try:
        with conn.cursor() as cursor:
            query = """
            INSERT INTO users (name)
            VALUES (%s)
            """
            
            # insert all users at once
            cursor.executemany(query, user_names)
        
        # commit the transaction
        conn.commit()
        return {"detail": f"{num_users} users signed up on WhatsApp!!"}
    
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()


@app.post('/messages/create')
def create_messages(num_msgs: int):
    # I need to create messages for valid users only
    try:
        user_ids = get_valid_user_ids()

        if not user_ids:
            raise HTTPException(status_code=400, detail='No valid users in the database.')
        
        user_ids = random.sample(user_ids, 10)
        insert_random_messages(num_msgs, user_ids)

        return {"message": f"{num_msgs} random messages created successfully!!!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")


@app.post("/users/update_last_online_ts")
def update_last_online_ts(num_users: int):
    try:
        valid_user_ids = get_valid_user_ids()

        if not valid_user_ids:
            raise HTTPException(status_code=404, detail="No users found in the database.")
        
        # pick random users to update
        if num_users > len(valid_user_ids):
            raise HTTPException(status_code=400, detail=f"Cannot update {num_users} users. Only {len(valid_user_ids)} available.")
        
        selected_user_ids = random.sample(valid_user_ids, num_users)

        update_last_login_ts_for_users(selected_user_ids)
    
        return {"message": f"Last login timestamps updated for {num_users} random users.", "user_ids": selected_user_ids}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
