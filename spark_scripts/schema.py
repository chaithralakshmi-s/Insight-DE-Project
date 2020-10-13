from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType
import postgres


def get_events_schema():
    eventSchema =  StructType([
        StructField("global_event_id",StringType(),True),
        StructField("sql_date",StringType(),True),
        StructField("month_year",StringType(),True),
        StructField("year",StringType(),True),
        StructField("fraction_date",StringType(),True),
        StructField("actor1_code",StringType(),True),
        StructField("actor1_name",StringType(),True),
        StructField("actor1_country_code",StringType(),True),
        StructField("actor1_known_group_code",StringType(),True),
        StructField("actor1_ethnic_code",StringType(),True),
        StructField("actor1_religion1_code",StringType(),True),
        StructField("actor1_religion2_code",StringType(),True),
        StructField("actor1_type1_code",StringType(),True),
        StructField("actor1_type2_code",StringType(),True),
        StructField("actor1_type3_code",StringType(),True),
        StructField("actor2_code",StringType(),True),
        StructField("actor2_name",StringType(),True),
        StructField("actor2_country_code",StringType(),True),
        StructField("actor2_known_group_code",StringType(),True),
        StructField("actor2_ethnic_code",StringType(),True),
        StructField("actor2_religion1_code",StringType(),True),
        StructField("actor2_religion2_code",StringType(),True),
        StructField("actor2_type1_code",StringType(),True),
        StructField("actor2_type2_code",StringType(),True),
        StructField("actor2_type3_code",StringType(),True),
        StructField("is_root_event",StringType(),True),
        StructField("event_code",StringType(),True),
        StructField("event_base_code",StringType(),True),
        StructField("event_root_code",StringType(),True),
        StructField("quad_class",StringType(),True),
        StructField("goldstein_scale",StringType(),True),
        StructField("num_mentions",StringType(),True),
        StructField("num_sources",StringType(),True),
        StructField("num_articles",StringType(),True),
        StructField("avg_tone",StringType(),True),
        StructField("actor1_geo_type",StringType(),True),
        StructField("actor1_geo_full_name",StringType(),True),
        StructField("actor1_geo_country_code",StringType(),True),
        StructField("actor1_geo_adm1_code",StringType(),True),
        StructField("actor1_geo_adm2_code",StringType(),True),
        StructField("actor1_geo_lat",StringType(),True),
        StructField("actor1_geo_long",StringType(),True),
        StructField("actor1_geo_feature_id",StringType(),True),
        StructField("actor2_geo_type",StringType(),True),
        StructField("actor2_geo_full_name",StringType(),True),
        StructField("actor2_geo_country_code",StringType(),True),
        StructField("actor2_geo_adm1_code",StringType(),True),
        StructField("actor2_geo_adm2_code",StringType(),True),
        StructField("actor2_geo_lat",StringType(),True),
        StructField("actor2_geo_long",StringType(),True),
        StructField("actor2_geo_feature_id",StringType(),True),
        StructField("action_geo_type",StringType(),True),
        StructField("action_geo_full_name",StringType(),True),
        StructField("action_geo_country_code",StringType(),True),
        StructField("action_geo_adm1_code",StringType(),True),
        StructField("action_geo_adm2_code",StringType(),True),
        StructField("action_geo_lat",StringType(),True),
        StructField("action_geo_long",StringType(),True),
        StructField("action_geo_feature_id",StringType(),True),
        StructField("date_added",StringType(),True),
        StructField("source_url",StringType(),True)])
    return eventSchema

def transform_events_df(df):
    df = df.withColumn("global_event_id", df.global_event_id.cast("LONG"))
    df = df.withColumn("event_date", F.to_date(df.sql_date, format="yyyyMMdd"))
    
    df = df.withColumn("actor1_code", df.actor1_code.cast("STRING"))
    df = df.withColumn("actor1_name", df.actor1_name.cast("STRING"))
    df = df.withColumn("actor1_country_code", df.actor1_country_code.cast("STRING"))
    df = df.withColumn("actor1_known_group_code", df.actor1_known_group_code.cast("STRING"))
    df = df.withColumn("actor1_type1_code", df.actor1_type1_code.cast("STRING"))
    df = df.withColumn("actor1_type2_code", df.actor1_type2_code.cast("STRING"))
    df = df.withColumn("actor1_type3_code", df.actor1_type3_code.cast("STRING"))

    df = df.withColumn("actor2_code", df.actor2_code.cast("STRING"))
    df = df.withColumn("actor2_name", df.actor2_name.cast("STRING"))
    df = df.withColumn("actor2_country_code", df.actor2_country_code.cast("STRING"))
    df = df.withColumn("actor2_known_group_code", df.actor2_known_group_code.cast("STRING"))
    df = df.withColumn("actor2_type1_code", df.actor2_type1_code.cast("STRING"))
    df = df.withColumn("actor2_type2_code", df.actor2_type2_code.cast("STRING"))
    df = df.withColumn("actor2_type3_code", df.actor2_type3_code.cast("STRING"))

    df = df.withColumn("is_root_event", df.is_root_event.cast("INT"))
    df = df.withColumn("event_code", df.event_code.cast("STRING"))
    df = df.withColumn("event_base_code", df.event_base_code.cast("STRING"))
    df = df.withColumn("event_root_code", df.event_root_code.cast("STRING"))
    df = df.withColumn("goldstein_scale", df.goldstein_scale.cast("FLOAT"))
    df = df.withColumn("num_mentions", df.num_mentions.cast("INT"))
    df = df.withColumn("num_sources", df.num_sources.cast("INT"))
    df = df.withColumn("num_articles", df.num_articles.cast("INT"))
    df = df.withColumn("avg_tone", df.avg_tone.cast("FLOAT"))

    df = df.withColumn("source_url", df.source_url.cast("STRING"))

    return df
def get_event_mentions_schema():
    mentionSchema =  StructType([
        StructField("global_event_id",StringType(),True),
        StructField("event_time_date",StringType(),True),
        StructField("mention_time_date",StringType(),True),
        StructField("mention_type",StringType(),True),
        StructField("mention_source_name",StringType(),True),
        StructField("mention_identifier",StringType(),True),
        StructField("sentence_id",StringType(),True),
        StructField("actor1_char_offset",StringType(),True),
        StructField("actor2_char_offset",StringType(),True),
        StructField("action_char_offset",StringType(),True),
        StructField("in_raw_text",StringType(),True),
        StructField("confidence",StringType(),True),
        StructField("mention_doc_len",StringType(),True),
        StructField("mention_doc_tone",StringType(),True),
        StructField("mention_doc_translation_info",StringType(),True),
        StructField("extras",StringType(),True)])
    return mentionSchema

def transform_event_mentions_df( df):
    df = df.withColumn("global_event_id", df.global_event_id.cast("LONG"))
    df = df.withColumn("mention_type", df.mention_type.cast("STRING"))
    df = df.withColumn("mention_source_name", df.mention_source_name.cast("STRING"))
    df = df.withColumn("mention_identifier", df.mention_identifier.cast("STRING"))
    df = df.withColumn("actor1_char_offset", df.actor1_char_offset.cast("INT"))
    df = df.withColumn("actor2_char_offset", df.actor2_char_offset.cast("INT"))
    df = df.withColumn("action_char_offset", df.action_char_offset.cast("INT"))
    df = df.withColumn("confidence", df.confidence.cast("FLOAT"))
    df = df.withColumn("mention_doc_tone", df.mention_doc_tone.cast("FLOAT"))
    return df


def transform_tweeters_df(df):
    df = df.withColumn('user_id', df.user_id.cast('LONG'))
    df = df.withColumn('user_screen_name', df.user_screen_name.cast('STRING'))
    df = df.withColumn('followers_count', df.followers_count.cast('INT'))
    df = df.withColumn('friends_count', df.friends_count.cast('INT'))
    df = df.withColumn('listed_count', df.listed_count.cast('INT'))
    df = df.withColumn('favorite_count', df.favorite_count.cast('INT'))
    df = df.withColumn('statuses_count', df.statuses_count.cast('INT'))
    df = df.withColumn('user_location', df.user_location.cast('STRING'))
    return df

def transform_tweets_df(df):
    #df = df.withColumn('created_at',F.to_date(df.created_at, format="yyyyMMdd"))
    df = df.withColumn("created_at",F.to_date(df.created_at,  "EEE MMM dd HH:mm:ss Z yyyy"))

    df = df.withColumn('status_id', df.status_id.cast('LONG'))
    df = df.withColumn('user_id', df.user_id.cast('LONG'))
    df = df.withColumn('url', df.url.cast('STRING'))
    df = df.withColumn('reply_count', df.reply_count.cast('INT'))
    df = df.withColumn('retweet_count', df.retweet_count.cast('INT'))
    df = df.withColumn('quote_count', df.quote_count.cast('INT'))
    df = df.withColumn('favorite_count', df.favorite_count.cast('INT'))
    return df

def transform_retweets_df(df):
    df = df.withColumn('retweet_status_id', df.retweet_status_id.cast('LONG'))
    df = df.withColumn('retweet_src_status_id', df.retweet_src_status_id.cast('LONG'))
    return df     

def transform_quotes_df(df):
    df = df.withColumn('quote_status_id', df.quote_status_id.cast('LONG'))
    df = df.withColumn('quote_src_status_id', df.quote_src_status_id.cast('LONG'))
    return df  
