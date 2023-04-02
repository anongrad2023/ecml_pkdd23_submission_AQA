INSERT_CONTEXT = (
    "INSERT INTO `context`"
    "(`experiment_id`, `context_desc`, `context_query`)"
    "VALUES (%(exp_id)s, %(desc)s, %(query)s)")

INSERT_SUBREDDIT = (
    "INSERT IGNORE INTO `snd_subreddit`"
    "(`subreddit_id`, `subreddit_name`)"
    "VALUES (%(sub_id)s, %(sub_name)s)")

INSERT_USER_REDDIT = (
    "INSERT IGNORE INTO `user_reddit`"
    "(`user_reddit_id`, `user_reddit_name`)"
    "VALUES (%(user_id)s, %(user_name)s)")

INSERT_CONTENT_POST = (
    "INSERT IGNORE INTO `content_post`"
    "(`post_id`, `user_reddit_id`, `text`, `datetime`, `score`, `subreddit`)"
    "VALUES (%(post_id)s, %(user_id)s, %(text)s, %(datetime)s, %(score)s, %(sub_id)s)")

INSERT_CTX_CNT_POST = (
    "INSERT INTO `context_post`"
    "(`context_id`, `content_post_id`)"
    "VALUES (%(context_id)s, %(post_id)s)")

INSERT_CONTENT_COMMENT = (
    "INSERT IGNORE INTO `content_comment`"
    "(`comment_id`, `user_reddit_id`, `parent_id`,`text`, `datetime`, `score`, `subreddit`)"
    "VALUES (%(comment_id)s, %(user_id)s, %(parent_id)s, %(text)s, %(datetime)s, %(score)s, %(sub_id)s)")

INSERT_CTX_CNT_COMMENT = (
    "INSERT INTO `context_comment`"
    "(`context_id`, `content_comment_id`)"
    "VALUES (%(context_id)s, %(comment_id)s)")

INSERT_USER_TWITTER = (
    "INSERT IGNORE INTO `user_twitter`"
    "(`user_twitter_id`, `user_twitter_name`)"
    "VALUES (%(user_id)s, %(user_name)s)")

INSERT_CONTENT_TWEET = (
    "INSERT IGNORE INTO `content_tweet`"
    "(`tweet_id`, `user_twitter_id`, `text`, `datetime`, `retweets`)"
    "VALUES (%(tweet_id)s, %(user_id)s, %(text)s, %(datetime)s, %(retweets)s)")

INSERT_HASHTAG = (
    "INSERT INTO `snd_hashtags`"
    "(`hashtag`, `tweet_id`)"
    "VALUES (%(hashtag)s, %(tweet_id)s)")

INSERT_MENTIONED_USER = (
    "INSERT INTO `snd_mentioned_users`"
    "(`tweet_id`, `user_twitter_id`)"
    "VALUES (%(tweet_id)s, %(user_id)s)"
)

INSERT_CTX_CNT_TWEET = (
    "INSERT INTO `context_tweet`"
    "(`context_id`, `content_tweet_id`)"
    "VALUES (%(context_id)s, %(tweet_id)s)")
