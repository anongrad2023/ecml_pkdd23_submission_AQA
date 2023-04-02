import mysql.connector
from decouple import config
import csv
import praw
from psaw import PushshiftAPI
from ccao_tools import gather, sql
from tqdm import tqdm

# because i hate it when things muck up the tqdm bars
import warnings
warnings.filterwarnings('ignore')


praw = praw.Reddit("data_enrich", user_agent="data_project_ua")
reddit = PushshiftAPI(praw)

FN_TARGET_POSTS = "data/subreddit_specific_posts_05.csv"
EXP_ID = "targeted_subs_00"
DATABASE_NAME = "ccao_project_05"

# Create contexts in DB:
CONTEXTS = [
    ["mil_drones", "military and drones"],
    ["mil_tech", "military and tech"],
    ["tech_privacy", "Technology and privacy"],
    ["tech_fakenews", "Technology and fake news"],
    ["pol_biden", "The presidency of Joe Biden"],
    ["pol_pandemic", "Governments response to the pandemic"],
    ["pol_vaccines", "The effectiveness of vaccines."]
]
context_ids = dict()

cnx = mysql.connector.connect(user=config('DB_USER'), password=config('DB_PASSWORD'),
                              host=config('DB_HOST'), database=DATABASE_NAME)

for ctx_label, ctx_query in CONTEXTS:
    ctx_id = gather.create_context(cnx, EXP_ID, ctx_label, ctx_query)
    context_ids[ctx_label] = ctx_id
cnx.commit()

added = 0
with open(FN_TARGET_POSTS, 'r') as f:
    rows = list(csv.reader(f))
    csv_pb = tqdm(rows, unit="post")
    for row in csv_pb:
        ctx_label, subreddit, post_id, post_title, post_url = row
        comments = reddit.search_comments(subreddit=subreddit, link_id=post_id, limit=100)
        cur = cnx.cursor()
        for comment in comments:
            try:
                cur.execute(sql.INSERT_SUBREDDIT,
                            {'sub_id': comment.subreddit_id,
                             'sub_name': subreddit})
                cur.execute(sql.INSERT_USER_REDDIT,
                            {'user_id': comment.author.fullname,
                             'user_name': comment.author.name})
                cur.execute(sql.INSERT_CONTENT_COMMENT,
                            {'comment_id': comment.id,
                             'user_id': comment.author.fullname,
                             'parent_id': comment.parent_id,
                             'text': comment.body,
                             'datetime': comment.created_utc,
                             'score': comment.score,
                             'sub_id': comment.subreddit_id})
                cur.execute(sql.INSERT_CTX_CNT_COMMENT,
                            {'context_id': context_ids[ctx_label],
                             'comment_id': comment.id})
                cnx.commit()
                added += 1
                csv_pb.postfix = f" comments: {added}"
            except Exception as e:
                # print(e)
                pass
