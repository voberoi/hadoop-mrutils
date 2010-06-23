REGISTER udfs.jar;

tweets = LOAD '$TWEETS' USING PigStorage('\t') AS (
       ts:int,
       username:chararray,
       tweet:chararray
);

replies = FILTER tweets BY udfs.IsReply(tweet);

replies_with_source = FOREACH replies {
	GENERATE 
	 ts AS ts, 
	 username AS username, 
	 tweet AS tweet, 
	 udfs.GetSource(tweet) AS source;
}

tweets_replies = JOIN tweets BY username, replies_with_source BY source;
valid_tweets_replies = FILTER tweets_replies BY tweets::ts < replies_with_source::ts;

grouped_tweets_replies = GROUP valid_tweets_replies BY replies_with_source::tweet;
matched_tweet_replies = FOREACH grouped_tweets_replies {
	ordered = ORDER valid_tweets_replies BY tweets::ts DESC;
	last_tweet = LIMIT ordered 1;
	GENERATE FLATTEN(last_tweet.tweets::tweet), group;
}

STORE matched_tweet_replies INTO '$OUT';
