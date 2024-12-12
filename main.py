"""This script needed to get all of the posts.

This will help to select best performing (in terms of engagement).
Best posts can be promoted via Telegram ads or through
other traffic sources.
"""

import pandas as pd
import sqlite3

from get_posts import get_posts
from settings import CHANNEL_ADDRESS


get_posts()
con = sqlite3.connect('all_posts.sqlite')
df = pd.read_sql_query(f'SELECT * FROM {CHANNEL_ADDRESS}', con)
df.to_excel(f'{CHANNEL_ADDRESS}.xlsx', index=False)
con.close()
