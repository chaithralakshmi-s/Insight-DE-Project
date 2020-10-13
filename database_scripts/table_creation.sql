 DROP TABLE IF EXISTS events_delta CASCADE;
 DROP TABLE IF EXISTS event_mentions_delta CASCADE;
 DROP TABLE IF EXISTS tweeters_delta CASCADE;
 DROP TABLE IF EXISTS tweets_delta CASCADE;
 DROP TABLE IF EXISTS retweets_delta CASCADE;
 DROP TABLE IF EXISTS quotes_delta CASCADE;
   
/* *********************************************************************
						GDELT-EVENTS
********************************************************************* */
DROP TABLE IF EXISTS events CASCADE;

CREATE TABLE events
(
   global_event_id            BIGINT PRIMARY KEY,
   event_date                  DATE ,
   actor1_code                VARCHAR(1000) ,
   actor1_name                VARCHAR(1000) ,
   actor1_country_code        VARCHAR(1000) ,
   actor1_known_group_code    VARCHAR(1000) ,
   actor1_type1_code          VARCHAR(1000) ,
   actor1_type2_code          VARCHAR(1000) ,
   actor1_type3_code          VARCHAR(1000) ,
   actor2_code                VARCHAR(1000) ,
   actor2_name                VARCHAR(1000) ,
   actor2_country_code        VARCHAR(1000) ,
   actor2_known_group_code    VARCHAR(1000) ,
   actor2_type1_code          VARCHAR(1000) ,
   actor2_type2_code          VARCHAR(1000) ,
   actor2_type3_code          VARCHAR(1000) ,
   is_root_event              INTEGER ,
   event_code                 VARCHAR(1000) ,
   event_base_code            VARCHAR(1000) ,
   event_root_code            VARCHAR(1000) ,
   goldstein_scale            REAL ,
   num_mentions               INTEGER ,
   num_sources                INTEGER ,
   num_articles               INTEGER ,
   avg_tone                   REAL ,
   actor1_geo_type            INTEGER ,
   actor1_geo_full_name       VARCHAR(1000) ,
   actor1_geo_country_code    VARCHAR(1000),
   source_url varchar(1000)
)
;
grant all on events to test;

/* *********************************************************************
						GDELT-EVENT MENTIONS
********************************************************************* */
DROP TABLE IF EXISTS event_mentions CASCADE;



CREATE TABLE event_mentions
(
   global_event_id                 BIGINT ,
   mention_type                    VARCHAR(1000) ,
   mention_source_name             VARCHAR(1000) ,
   mention_identifier              VARCHAR(1000) ,
   actor1_char_offset              INT,
   actor2_char_offset              INT ,
   action_char_offset              INT,
   confidence                      REAL ,
   mention_doc_tone                REAL 
   );
grant all on event_mentions to test;

/* *********************************************************************
						TWITTER - USERS
********************************************************************* */


DROP TABLE IF EXISTS tweeters CASCADE;


   create table tweeters(
   user_id             BIGINT Primary key,
   user_screen_name    VARCHAR(1000)  ,
   followers_count     BIGINT DEFAULT 0,
   friends_count       BIGINT DEFAULT 0,
   listed_count        BIGINT DEFAULT 0,
   favorite_count     BIGINT DEFAULT 0,
   statuses_count      BIGINT DEFAULT 0,
   user_location       VARCHAR(1000)  
   );
   GRANT ALL ON tweeters to test;
   
  /* *********************************************************************
						TWITTER - TWEETS
********************************************************************* */


DROP TABLE IF EXISTS tweets CASCADE;


CREATE TABLE tweets
(
   status_id             BIGINT PRIMARY KEY ,
   user_id               BIGINT NOT NULL,
   url                   VARCHAR(50000)  NOT NULL,
   reply_count       BIGINT DEFAULT 0 ,
   retweet_count     BIGINT DEFAULT 0 ,
   quote_count       BIGINT  DEFAULT 0,
   favorite_count    BIGINT  DEFAULT 0 ,
   avg_tweet_score       REAL DEFAULT 0,
   created_at            TIMESTAMP 
   
);
GRANT ALL ON tweets to test;


/* *********************************************************************
						TWITTER - RETWEETS
********************************************************************* */

DROP TABLE IF EXISTS retweets CASCADE;


CREATE TABLE retweets
(
   retweet_status_id    BIGINT ,
   retweet_src_status_id      BIGINT ,
   PRIMARY KEY(retweet_status_id,retweet_src_status_id)
);

grant all on retweets to test;
  
/* *********************************************************************
						TWITTER - QUOTES
********************************************************************* */

DROP TABLE IF EXISTS quotes CASCADE;

CREATE TABLE quotes
(
   quote_status_id    BIGINT ,
   quote_src_status_id      BIGINT ,
   PRIMARY KEY(quote_status_id,quote_src_status_id)
);
grant all on quotes to test;

