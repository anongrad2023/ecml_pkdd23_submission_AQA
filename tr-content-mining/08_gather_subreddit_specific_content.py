import praw
from psaw import PushshiftAPI

import csv
from tqdm import tqdm

# Political: conservative, liberal, politics
# Technology: software, computers, tech, apple, android
# Military: army, navy, airforce, usmc, military, veterans
# Unrelated: pics, cats, juggling, woodworking

subreddits = ["conservative", "liberal", "politics",                       # Political
              "software", "computers", "tech", "apple", "android",         # Technology
              "army", "navy", "airforce", "usmc", "military", "veterans",  # Military
              "pics", "cats", "juggling", "woodworking"]                   # Unrelated

context_queries = [
    ["mil_drones",    "The military and its use of drones"],
    ["mil_tech",      "The military and its use of tech"],
    ["tech_privacy",  "Technology and invasion of privacy"],
    ["tech_fakenews", "Technology and its role in fake news"],
    ["pol_biden",     "The presidency of Joe Biden"],
    ["pol_pandemic",  "The governments response to the pandemic"],
    ["pol_vaccines",  "The effectiveness of vaccines."],
    ["pol_vaccines",  "The use of vaccines."]
]

# ["mil_drones",    "The military and its use of drones"],
# ["mil_tech",      "The military and its use of tech"],
# ["tech_privacy",  "Technology and invasion of privacy"],
# ["tech_fakenews", "Technology and its role in fake news"],
# ["pol_biden",     "The presidency of Joe Biden"],
# ["pol_pandemic",  "The governments response to the pandemic"],
# ["pol_vaccines",  "The effectiveness of vaccines."],

MAX_TRIES = 1000
MIN_SCORE = 10
MIN_COMMENTS = 5
NUM_POSTS = 5
FN_OUT = "data/subreddit_specific_posts_05.csv"

reddit = PushshiftAPI()

posts = []
outer_pb = tqdm(context_queries, unit=" queries", position=0)
for ctx_label, ctx_query in outer_pb:
    for subreddit in tqdm(subreddits, leave=False, unit=" sub"):
        results = reddit.search_submissions(q=ctx_query,
                                            subreddit=subreddit,
                                            filter=['url', 'author', 'title', 'subreddit', 'id'],
                                            limit=NUM_POSTS,
                                            sort='desc',
                                            sort_type='num_comments',
                                            after="2y")
        for post in results:
            posts.append([ctx_label, subreddit, post.id, post.title, post.url])

with open(FN_OUT, 'w') as f:
    csv.writer(f).writerows(posts)
