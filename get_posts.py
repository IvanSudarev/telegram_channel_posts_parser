"""This module get all the posts from the Telegram channel.

It provides: views count, replies count, forwards count,
reactions count, post text, post id and post url
"""

import sqlite3
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest

from settings import API_ID, API_HASH, CHANNEL_ADDRESS, POSTS_LIMIT


def get_posts(
        channel_address=CHANNEL_ADDRESS,
        post_limit=POSTS_LIMIT):
    """Get POST_LIMIT of posts from channel with CHANNEL_ADDRESS."""

    def get_last_pk(cur):
        cur.execute(f'SELECT MAX(id) FROM {channel_address}')
        return cur.fetchone()[0]

    con = sqlite3.connect('all_posts.sqlite')
    cur = con.cursor()

    # Create empty table for channel posts
    cur.executescript(f'''
        CREATE TABLE IF NOT EXISTS {channel_address}(
        id INTEGER PRIMARY KEY,
        post_text TEXT,
        views_count INTEGER,
        reactions_count INTEGER,
        repost_count INTEGER,
        replies_count INTEGER,
        post_id INTEGER,
        post_url TEXT
    );''')

    # Get all existing post ids
    cur.execute(f'SELECT post_id FROM {channel_address}')
    rows = cur.fetchall()
    existing_posts = [row[0] for row in rows]

    new_posts = []

    # Connect to Telegram
    client = TelegramClient('session_name', API_ID, API_HASH)

    last_pk = get_last_pk(cur)
    all_messages = []

    async def get_channel_messages(channel_address):
        # Start the client session
        await client.start()

        # Get the channel entity
        channel = await client.get_entity(channel_address)

        # Set initial parameters

        offset_id = 0
        limit = 100  # Fetch messages in batches of 100

        while len(all_messages) < post_limit:
            # Fetch message history
            history = await client(GetHistoryRequest(
                peer=channel,
                limit=limit,
                offset_date=None,
                offset_id=offset_id,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))

            # If there are no more messages, break the loop
            if not history.messages:
                break

            # Add fetched messages to the list
            all_messages.extend(history.messages)

            # Print progress (optional)
            print(f"Fetched {len(all_messages)} messages...")

            # Set the offset_id for the next batch to the id
            # of the last message
            offset_id = history.messages[-1].id

    with client:
        client.loop.run_until_complete(get_channel_messages(CHANNEL_ADDRESS))

    last_pk = get_last_pk(cur)
    last_pk = last_pk if last_pk else 0

    for message in all_messages:

        # If metric is not available it will be count as 0
        views = message.views if message.views else 0

        reactions = 0
        if message.reactions:
            for reaction in message.reactions.results:
                reactions += reaction.count

        replies = message.replies.replies if message.replies else 0
        repost_count = message.forwards if message.forwards else 0

        # Add new messages to the list
        if message.id not in existing_posts:
            last_pk += 1
            new_posts.append((
                last_pk,
                message.message,
                views,
                reactions,
                repost_count,
                replies,
                message.id,
                f'https://t.me/{channel_address}/{message.id}'
                )
            )
    cur.executemany((
        f'INSERT INTO {channel_address}'
        ' VALUES(?, ?, ?, ?, ?, ?, ?, ?);'),
        new_posts)

    con.commit()
    con.close()
