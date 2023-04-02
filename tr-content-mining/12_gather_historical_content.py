import mysql.connector
from decouple import config
import csv
import praw
from psaw import PushshiftAPI
from tqdm import tqdm
from os.path import exists
from datetime import timedelta, datetime

import warnings
warnings.filterwarnings('ignore')  # Hackiest way to prevent things mucking up tqdm bars.

NUM_DAYS = 7

DIR_UD_DATE = "data/historical_user_data"
DATABASE_NAME = "ccao_project_05"

SQL_GET_USER_DATES = """
SELECT user_reddit.user_reddit_name, user_reddit.user_reddit_id, comment_id from content_comment
JOIN context_comment on comment_id=content_comment_id
JOIN user_reddit on user_reddit.user_reddit_id=content_comment.user_reddit_id
WHERE context_id in (206, 211, 209, 205, 207, 208);
"""

cnx = mysql.connector.connect(user=config('DB_USER'), password=config('DB_PASSWORD'),
                              host=config('DB_HOST'), database=DATABASE_NAME)
cur = cnx.cursor()
cur.execute(SQL_GET_USER_DATES)

praw = praw.Reddit("data_enrich", user_agent="data_project_ua")
reddit = PushshiftAPI(praw)

dt_delta = timedelta(days=NUM_DAYS)

for result in tqdm(cur.fetchall(), unit="user"):
    user_name, user_id, comment_id = result

    comment = praw.comment(comment_id)
    dt_before = datetime.fromtimestamp(comment.created_utc)
    dt_after  = dt_before-dt_delta
    t_before  = int(dt_before.timestamp())
    t_after   = int(dt_after.timestamp())

    # Check if we've done this user-date pair.
    fn_check = f"{DIR_UD_DATE}/{user_name}_{t_before}.csv"
    if exists(fn_check):
        continue

    h_comments = reddit.search_comments(author=user_name, after=t_after, before=t_before)
    comment_rows = []
    for c in h_comments:
        comment_rows.append([
            user_id,
            user_name,
            comment_id,
            c.id,
            c.score,
            c.subreddit,
            c.subreddit_id,
            c.created_utc,
            c.body
        ])

    # Name each file as <user-id>_<createdat_utc>.csv
    with open(f"{DIR_UD_DATE}/{user_name}_{t_before}.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "user_name", "src_comment_id",
                         "hc_comment_id", "hc_score", "hc_subreddit", "hc_subreddit_id",
                         "hc_created_at", "hc_body"])
        writer.writerows(comment_rows)
