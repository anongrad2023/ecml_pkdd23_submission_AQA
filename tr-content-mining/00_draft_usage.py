import snscrape.modules.twitter as twitter
import snscrape.modules.reddit as reddit
import pandas as pd
import csv
from os import path, mkdir

CONTEXTS = {
    "vaccines":   "the effectiveness of vaccines",
    "drones":     "the USA's use of military drones",
    "world_usa":  "how the world views the USA",
    "pres_trump": "the presidency of donald trump",
    "pres_biden": "the presidency of joe biden",
    "russia_ukraine": "russia's role in the war in ukraine",
}

OUTPUT_DIR = "data/example_pairs_00"
MAX_EXAMPLES = 200

BATCH_SIZE = 100
COLS_TWITTER = ["date", "tweet_id", "text"]
COLS_REDDIT = ["date", ""]


def general_twitter(context_str, context_label, output_dir, max_examples):
    if not path.exists(output_dir):
        mkdir(output_dir)

    search_str = "{}".format(context_str)
    tweet_res = twitter.TwitterSearchScraper(search_str).get_items()

    def save_twitter_batch(batch):
        raw_df_data = []
        for cont in batch:
            raw_df_data.append([cont.date, cont.id, cont.content])
        df_batch = pd.DataFrame(raw_df_data, columns=COLS_TWITTER)
        df_batch.to_csv(fn_content, mode='a', index=False, quoting=csv.QUOTE_NONNUMERIC, header=False)

    content_batch = []
    fn_content = "{}/{}_tweets.csv".format(output_dir, context_label)
    for c, content in enumerate(tweet_res):

        if c > max_examples: break
        content_batch.append(content)
        if len(content_batch) >= BATCH_SIZE:
            save_twitter_batch(content_batch)
            content_batch = []
    save_twitter_batch(content_batch)


def general_reddit(context_str, context_label, output_dir, max_examples):
    if not path.exists(output_dir):
        mkdir(output_dir)

    search_str = "{}".format(context_str)
    reddit_res = reddit.RedditSearchScraper(search_str, comments=False).get_items()

    def save_reddit_batch(batch):
        df_batch = pd.DataFrame(batch, columns=COLS_TWITTER)
        df_batch.to_csv(fn_content, mode='a', index=False, quoting=csv.QUOTE_NONNUMERIC, header=False)

    content_batch = []
    fn_content = "{}/{}_posts.csv".format(output_dir, context_label)
    for c, content in enumerate(reddit_res):
        if c > max_examples: break
        try:
            row = [content.id, content.author, content.selftext]
            content_batch.append(row)

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            pass

        if len(content_batch) >= BATCH_SIZE:
            save_reddit_batch(content_batch)
            content_batch = []
    save_reddit_batch(content_batch)


for c_label, c_str in CONTEXTS.items():
    general_twitter(c_str, c_label, OUTPUT_DIR, MAX_EXAMPLES)
    general_reddit(c_str, c_label, OUTPUT_DIR, MAX_EXAMPLES)
