from pyspark.sql.types import *
from pyspark.sql import SparkSession
import logging
import schema
from spark_setup import run_spark
from postgresql_functions import write_to_db
from postgresql_functions import write_to_tweeters
from postgresql_functions import write_to_tweets
from postgresql_functions import write_to_retweets
from postgresql_functions import write_to_quotes
import time 
def process_tweeters(spark,sqlctx,df):
    df.createOrReplaceTempView("twitter_vw")

    df_users_1=spark.sql("select user_id, max(user_screen_name) user_screen_name, \
                                     MAX(followers_count) AS followers_count, \
                                    MAX(friends_count) AS friends_count , MAX(listed_count) AS listed_count , \
                                    MAX(favorite_count) AS favorite_count ,MAX(statuses_count) AS statuses_count \
                                    , MAX(user_location) AS user_location \
                                    FROM twitter_vw where user_id is not null group by user_id  ")
    

    df_users_2=df_users_1.dropDuplicates()

    df_users_3=schema.transform_tweeters_df(df_users_2)

    table = "tweeters_delta"
    mode = "overwrite"
    

    write_to_db(df_users_3, table, mode)
    write_to_tweeters()

def process_tweets(spark,sqlctx,df):
    df.createOrReplaceTempView("twitter_vw")

    #df_3.createOrReplaceTempView("twitter_vw")

    df_tweets_1=spark.sql("select status_id,max(user_id) user_id,max(created_at) as created_at, \
                                max(url)as url,max(reply_count) as reply_count, \
                                max(retweet_count) as retweet_count,max(quote_count) as quote_count, \
                                max(favorite_count) as favorite_count \
                            from twitter_vw where url is not null \
                            group by status_id")

    df_tweets_2=df_tweets_1.dropDuplicates()

    df_tweets_3=schema.transform_tweets_df(df_tweets_2)

    table = "tweets_delta"
    mode = "overwrite"

    write_to_db(df_tweets_3, table, mode)

    write_to_tweets()

def process_retweets(spark,sqlctx,df):
    df.createOrReplaceTempView("twitter_vw")

    df_retweets_1 = spark.sql("SELECT distinct status_id as retweet_status_id,retweet_src_id as retweet_src_status_id \
                            from  twitter_vw where   retweet_src_id is not null and status_id is not null") 
    df_retweets_2=df_retweets_1.dropDuplicates()
    df_retweets_3 = schema.transform_retweets_df(df_retweets_2)
    table = "retweets_delta"
    mode = "overwrite"
    write_to_db(df_retweets_3, table, mode)
    write_to_retweets()
    
def process_quotes(spark,sqlctx,df):
    df.createOrReplaceTempView("twitter_vw")

    df_quotes_1 = spark.sql("SELECT distinct status_id as quote_status_id,quote_src_id as quote_src_status_id \
                            from  twitter_vw where   quote_src_id is not null and status_id is not null") 
    df_quotes_2=df_quotes_1.dropDuplicates()
    df_quotes_3 = schema.transform_quotes_df(df_quotes_2)
    table = "quotes_delta"
    mode = "overwrite"
    write_to_db(df_quotes_3, table, mode)
    write_to_quotes()
    

def create_tweets_df(spark,sqlctx,df_1):    

    df_1.createOrReplaceTempView("twitter_src_vw")
    df_1.dropDuplicates()
    df_2 = spark.sql("select created_at, \
                                id as status_id \
                                ,user.id as user_id \
                                ,NVL(user.screen_name,'') as user_screen_name \
                                , user.followers_count as followers_count\
                                , user.listed_count as listed_count \
                                , user.favourites_count as favorites_count\
                                ,user.statuses_count as statuses_count\
                                ,user.friends_count as friends_count\
                                , regexp_replace(user.location, '[^a-zA-Z0-9]+', '') as user_location  \
                                , case when entities.urls.expanded_url[0] is not null then LOWER(entities.urls.expanded_url[0])  \
                                        else LOWER(entities.urls.url[0]) end  url \
                                ,in_reply_to_user_id as in_reply_to_user_id \
                                ,in_reply_to_status_id as in_reply_to_status_id \
                                ,in_reply_to_screen_name as in_reply_to_screen_name \
                                ,reply_count as reply_count  \
                                ,retweet_count as retweet_count \
                                ,quote_count as quote_count \
                                ,favorite_count as favorite_count \
                                ,retweeted_status.id retweet_src_id \
                            ,retweeted_status.user.id as retweet_user_id \
                            ,retweeted_status.user.screen_name as retweet_user_screen_name \
                            ,quoted_status.id quote_src_id \
                            ,quoted_status.user.id as quote_user_id \
                            ,quoted_status.user.screen_name as quote_user_screen_name \
                            FROM twitter_src_vw where  id is not null and user.id is not null AND lang='en'")
    
    df_3= df_2.dropDuplicates()
    return df_3

def read_twitter_files(spark,sqlctx):
    twitter_file="s3a://ia-tweets/202003/01/10/*"
    df_1 = sqlctx.read. \
            option('encoding', 'UTF-8'). \
            option("mode", "DROPMALFORMED"). \
            json(twitter_file)
    return df_1

def main():
    initial_time= time.time()
    spark,sqlctx = run_spark("twitter","9gb")
    df_1 = read_twitter_files(spark,sqlctx)
    df_2=create_tweets_df(spark,sqlctx,df_1)
    print("********************* START - START TWEETERS *******************************")
    
    users_start_time = time.time()
   # process_tweeters(spark,sqlctx,df_2)
    users_end_time = time.time()
    print("process_tweeters" ,users_end_time-users_start_time,"sec", (users_end_time-users_start_time)/60,"mins" )

    print("********************* END TWEETERS - START TWEETS *******************************")
    tweets_start_time = time.time()
    process_tweets(spark,sqlctx,df_2)
    tweets_end_time = time.time()
    print("process_tweets" ,  tweets_end_time-tweets_start_time,"sec", (tweets_end_time-tweets_start_time)/60,"mins")
    print("********************* END TWEETS / START RETWEETS*******************************")

    retweets_start_time = time.time()
  #  process_retweets(spark,sqlctx,df_2)
    retweets_end_time = time.time()
    print("process_retweets" , retweets_end_time-retweets_start_time,"sec", (retweets_end_time-retweets_start_time)/60,"mins")
    print("********************* END RETWEETS /START QUOTES *******************************")
    
    quotes_start_time = time.time()
  #  process_quotes(spark,sqlctx,df_2)
    quotes_end_time = time.time()
    final_time=time.time()
    print("process_quotes" ,quotes_end_time-quotes_start_time,"sec", (quotes_end_time-quotes_start_time)/60,"mins" )
    print("********************* END QUOTES / FINAL END *******************************")

    print("process_tweeters" ,users_end_time-users_start_time,"sec", (users_end_time-users_start_time)/60,"mins" )

    print("process_tweets" ,  tweets_end_time-tweets_start_time,"sec", (tweets_end_time-tweets_start_time)/60,"mins")
    print("process_retweets" , retweets_end_time-retweets_start_time,"sec", (retweets_end_time-retweets_start_time)/60,"mins")
    print("process_quotes" ,quotes_end_time-quotes_start_time,"sec", (quotes_end_time-quotes_start_time)/60,"mins" )
    print("* Total Time" ,final_time-initial_time,"sec", (final_time-initial_time)/60,"mins" )

    
    print("****************************************************")
    print("****************************************************")
    print("****************************************************")
    print()
    print("               Twitter                      ")
    print()

    print("****************************************************")
    print("****************************************************")
    print("****************************************************")
if __name__ == '__main__':
    main()
