import snscrape.modules.twitter as twitter
import snscrape.modules.reddit as reddit

from . import sql


def create_context(db_cnx, exp_id, desc, query):
    cur = db_cnx.cursor()
    ctx_data = {
        'exp_id': exp_id,
        'desc': desc,
        'query': query
    }
    cur.execute(sql.INSERT_CONTEXT, ctx_data)
    db_cnx.commit()  # NOTE - Is this the best way to do this?
    return cur.lastrowid


def basic_query_post(db_cnx, context_id, query, max_results=5):
    search_str = f"{query}"
    reddit_res = reddit.RedditSearchScraper(search_str, comments=False).get_items()
    cur = db_cnx.cursor()
    for c, post in enumerate(reddit_res):
        if c > max_results: break
        try:
            # The order matters due to some foreign key constraints.
            cur.execute(sql.INSERT_SUBREDDIT,
                        {'sub_id': post.subreddit_id,
                         'sub_name': post.subreddit})
            cur.execute(sql.INSERT_USER_REDDIT,
                        {'user_id': post.author_id,
                         'user_name': post.author})
            cur.execute(sql.INSERT_CONTENT_POST,
                        {'post_id': post.id,
                         'user_id': post.author_id,
                         'text': post.selftext,
                         'datetime': post.date,
                         'score': post.score,
                         'sub_id': post.subreddit_id})
            cur.execute(sql.INSERT_CTX_CNT_POST,
                        {'context_id': context_id,
                         'post_id': post.id})
            db_cnx.commit()
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            # TODO - Verbose logging of e?
            raise e


def basic_query_comment(db_cnx, context_id, query, max_results=5):
    search_str = f"{query}"
    reddit_res = reddit.RedditSearchScraper(search_str, submissions=False).get_items()
    cur = db_cnx.cursor()
    for c, comment in enumerate(reddit_res):
        if c > max_results: break
        try:
            # The order matters due to some foreign key constraints.
            cur.execute(sql.INSERT_SUBREDDIT,
                        {'sub_id': comment.subreddit_id,
                         'sub_name': comment.subreddit})
            cur.execute(sql.INSERT_USER_REDDIT,
                        {'user_id': comment.author_id,
                         'user_name': comment.author})
            cur.execute(sql.INSERT_CONTENT_COMMENT,
                        {'comment_id': comment.id,
                         'user_id': comment.author_id,
                         'parent_id': comment.parentId,
                         'text': comment.body,
                         'datetime': comment.date,
                         'score': comment.score,
                         'sub_id': comment.subreddit_id})
            cur.execute(sql.INSERT_CTX_CNT_COMMENT,
                        {'context_id': context_id,
                         'comment_id': comment.id})
            db_cnx.commit()
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            # TODO - Verbose logging of e?
            raise e


def basic_query_tweet(db_cnx, context_id, query, max_results=5):
    search_str = f"{query}"  # NOTE - I was messing around with query flags.
    tweet_res = twitter.TwitterSearchScraper(search_str).get_items()
    cur = db_cnx.cursor()
    for c, tweet in enumerate(tweet_res):
        if c > max_results: break
        try:
            cur.execute(sql.INSERT_USER_TWITTER,
                        {'user_id':   tweet.user.id,
                         'user_name': tweet.user.username})
            cur.execute(sql.INSERT_CONTENT_TWEET,
                        {'tweet_id': tweet.id,
                         'user_id':  tweet.user.id,
                         'text':     tweet.rawContent,
                         'datetime': tweet.date,
                         'retweets': tweet.retweetCount})

            for hashtag in tweet.hashtags:
                cur.execute(sql.INSERT_HASHTAG,
                            {'hashtag': hashtag,
                             'tweet_id': tweet.id})

            for mentioned_user in tweet.mentionedUsers:
                cur.execute(sql.INSERT_USER_TWITTER,
                            {'user_id': mentioned_user.id,
                             'user_name': mentioned_user.username})
                cur.execute(sql.INSERT_MENTIONED_USER,
                            {'tweet_id': tweet.id,
                             'user_id': mentioned_user.id})

            cur.execute(sql.INSERT_CTX_CNT_TWEET,
                        {'context_id': context_id,
                         'tweet_id': tweet.id})

            db_cnx.commit()

        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            # TODO - Verbose logging of e?
            pass


