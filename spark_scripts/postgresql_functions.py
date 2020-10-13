from pyspark.sql import DataFrameWriter
import os
import configparser
import config
import logging
import psycopg2
def connect_to_db(database):
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.postgresql/config'))
    db_host=config.get('default','db_host')
    db_port=config.get('default','db_port')
    db_property=dict()
    db_property['user'] = config.get('default', 'db_user')
    db_property['password'] = config.get('default', 'db_pass')
    db_property['driver']=config.get('default', 'db_driver')
    db_property['url']= 'jdbc:postgresql://{host}:{port}/{db}'.format(host=db_host,port=db_port, db=database)
    return db_property
       
def write_to_db(df, table, mode ='append',database ='project_db', schema='public'):    
    db_property=connect_to_db(database)
    #Write to database
    writer = DataFrameWriter(df)
    writer.jdbc(db_property['url'], table, mode, db_property)




def read_from_db():
    print("reading")

def connectSQLServer(db_database):
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.postgresql/config'))
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.postgresql/config'))
    db_host=config.get('default','db_host')
    db_port=config.get('default','db_port')
    db_user = config.get('default', 'db_user')
    db_password = config.get('default', 'db_pass')


    conn = psycopg2.connect(database=db_database,user=db_user,password=db_password,host=db_host,port=db_port)
    return conn

def write_to_events(database ="project_db"):
    conn = connectSQLServer(database)
    cur = conn.cursor()

    update_sql='''update  events SET 
       event_date                 =   ed.event_date                 ,
       actor1_code              =   ed.actor1_code              ,
       actor1_name              =   ed.actor1_name              ,
       actor1_country_code      =   ed.actor1_country_code      ,
       actor1_known_group_code  =   ed.actor1_known_group_code  ,
       actor1_type1_code        =   ed.actor1_type1_code        ,
       actor1_type2_code        =   ed.actor1_type2_code        ,
       actor1_type3_code        =   ed.actor1_type3_code        ,
       actor2_code              =   ed.actor2_code              ,
       actor2_name              =   ed.actor2_name              ,
       actor2_country_code      =   ed.actor2_country_code      ,
       actor2_known_group_code  =   ed.actor2_known_group_code  ,
       actor2_type1_code        =   ed.actor2_type1_code        ,
       actor2_type2_code        =   ed.actor2_type2_code        ,
       actor2_type3_code        =   ed.actor2_type3_code        ,
       is_root_event            =   ed.is_root_event            ,
       event_code               =   ed.event_code               ,
       event_base_code          =   ed.event_base_code          ,
       event_root_code          =   ed.event_root_code          ,
       goldstein_scale          =   ed.goldstein_scale          ,
       num_mentions             =   ed.num_mentions             ,
       num_sources              =   ed.num_sources              ,
       num_articles             =   ed.num_articles             ,
       avg_tone                 =   ed.avg_tone                 ,
       source_url               =   ed.source_url
        from events_delta ed
       where exists (select * from events e where e.global_event_id=ed.global_event_id)'''
    cur.execute(update_sql)
    updated_rows = cur.rowcount
    print("updated row count = ",updated_rows)
    insert_sql = '''INSERT INTO events
        ( global_event_id         ,
        event_date                ,
        actor1_code             ,
        actor1_name             ,
        actor1_country_code     ,
        actor1_known_group_code ,
        actor1_type1_code       ,
        actor1_type2_code       ,
        actor1_type3_code       ,
        actor2_code             ,
        actor2_name             ,
        actor2_country_code     ,
        actor2_known_group_code ,
        actor2_type1_code       ,
        actor2_type2_code       ,
        actor2_type3_code       ,
        is_root_event           ,
        event_code              ,
        event_base_code         ,
        event_root_code         ,
        goldstein_scale         ,
        num_mentions            ,
        num_sources             ,
        num_articles            ,
        avg_tone                ,
        source_url) 
                select
        ed.global_event_id,
        ed.event_date          ,
        ed.actor1_code              ,
        ed.actor1_name              ,
        ed.actor1_country_code      ,
        ed.actor1_known_group_code  ,
        ed.actor1_type1_code        ,
        ed.actor1_type2_code        ,
        ed.actor1_type3_code        ,
        ed.actor2_code              ,
        ed.actor2_name              ,
        ed.actor2_country_code      ,
        ed.actor2_known_group_code  ,
        ed.actor2_type1_code        ,
        ed.actor2_type2_code        ,
        ed.actor2_type3_code        ,
        ed.is_root_event            ,
        ed.event_code               ,
        ed.event_base_code          ,
        ed.event_root_code          ,
        ed.goldstein_scale          ,
        ed.num_mentions             ,
        ed.num_sources              ,
        ed.num_articles             ,
        ed.avg_tone                 ,
        ed.source_url    
        from events_delta ed where  not exists
        (select * from events e where ed. global_event_id =e.global_event_id)'''
    cur.execute(insert_sql)
    inserted_rows = cur.rowcount

    print("inserted row count = ",inserted_rows)

    conn.commit()
    cur.close()
def write_to_event_mentions(database ="project_db"):
    conn = connectSQLServer(database)
    cur = conn.cursor()

    update_sql='''update  event_mentions SET 
                   confidence = emd.confidence,
                   mention_doc_tone = emd.mention_doc_tone
                    from event_mentions_delta emd
                       where exists 
                       (select 1 from event_mentions em where 
                           em.global_event_id = emd.global_event_id and 
                           em.mention_type = emd.mention_type and 
                           em.mention_source_name = emd.mention_source_name and 
                           em.mention_identifier =  emd.mention_identifier and 
                           em.actor1_char_offset = emd.actor1_char_offset and 
                           em.actor2_char_offset = emd.actor2_char_offset and 
                           em.action_char_offset = emd.action_char_offset)'''
    cur.execute(update_sql)
    updated_rows = cur.rowcount
    print("updated row count = ",updated_rows)
    insert_sql = '''INSERT INTO event_mentions
                      (global_event_id     ,
                       mention_type        ,
                       mention_source_name ,
                       mention_identifier  ,
                       actor1_char_offset  ,
                       actor2_char_offset  ,
                       action_char_offset  ,
                       confidence         ,
                       mention_doc_tone ) 
                         select emd.global_event_id     ,
                                emd.mention_type        ,
                                emd.mention_source_name ,
                                emd.mention_identifier  ,
                                emd.actor1_char_offset  ,
                                emd.actor2_char_offset  ,
                                emd.action_char_offset  ,
                                emd.confidence         ,
                                emd.mention_doc_tone               
                       from event_mentions_delta emd where  not exists
                            (select 1 from event_mentions em where 
                                       em.global_event_id = emd.global_event_id and 
                                       em.mention_type = emd.mention_type and 
                                       em.mention_source_name = emd.mention_source_name and 
                                       em.mention_identifier =  emd.mention_identifier and 
                                       em.actor1_char_offset = emd.actor1_char_offset and 
                                       em.actor2_char_offset = emd.actor2_char_offset and 
                                       em.action_char_offset = emd.action_char_offset)'''
    cur.execute(insert_sql)
    inserted_rows = cur.rowcount

    print("inserted row count = ",inserted_rows)

    conn.commit()
    cur.close()
    
       
def write_to_tweeters(database ="project_db"):
    conn = connectSQLServer(database)
    cur = conn.cursor()

    update_sql='''update  tweeters SET 
                          user_screen_name=td.user_screen_name    ,
                          followers_count =td.followers_count    , 
                          friends_count =td.friends_count      , 
                          listed_count=td.listed_count        , 
                          favorite_count=td.favorite_count     , 
                          statuses_count=td.statuses_count      , 
                          user_location=td.user_location  
                              from tweeters_delta td
                                      where exists(
                                          select 1 from tweeters t where t.user_id=td.user_id
                                          )'''
    cur.execute(update_sql)
    updated_rows = cur.rowcount
    print("updated row count = ",updated_rows)
    insert_sql = '''INSERT INTO tweeters
                                   (   user_id             , 
                                       user_screen_name    ,
                                       followers_count     , 
                                       friends_count       , 
                                       listed_count        , 
                                       favorite_count     , 
                                       statuses_count      , 
                                       user_location      
                                    ) 
                                 select distinct td.user_id             , 
                                        td.user_screen_name    ,
                                        td.followers_count     , 
                                        td.friends_count       , 
                                        td.listed_count        , 
                                        td.favorite_count     , 
                                        td.statuses_count      , 
                                        td.user_location                
                       from tweeters_delta td where  not exists
                            (select 1 from tweeters t where  t.user_id=td.user_id)'''
    cur.execute(insert_sql)
    inserted_rows = cur.rowcount

    print("inserted row count = ",inserted_rows)

    conn.commit()
    cur.close()
    
def write_to_tweets(database ="project_db"):
    conn = connectSQLServer(database)
    cur = conn.cursor()

    update_sql='''update  tweets SET 
                             user_id        =td.user_id         ,
                             url            =td.url             ,
                             reply_count    =td.reply_count     ,
                             retweet_count  =td.retweet_count   ,
                             quote_count    =td.quote_count     ,
                             favorite_count =td.favorite_count  ,
                             avg_tweet_score=td.reply_count+td.retweet_count+td.quote_count +td.favorite_count ,
                             created_at     =td.created_at          
                              from tweets_delta td
                                      where exists(
                                          select 1 from tweets t where t.status_id=td.status_id
                                          )'''
    cur.execute(update_sql)
    updated_rows = cur.rowcount
    print("updated row count = ",updated_rows)
    insert_sql = '''INSERT INTO tweets
                                   (       status_id       ,
                                           user_id         ,
                                           url             ,
                                           reply_count     ,
                                           retweet_count   ,
                                           quote_count     ,
                                           favorite_count                                      ) 
                                 select distinct td. status_id       ,
                                            td. user_id         ,
                                            td. url             ,
                                            td. reply_count     ,
                                            td. retweet_count   ,
                                            td. quote_count     ,
                                            td. favorite_count 
                       from tweets_delta td where  not exists
                            (select 1 from tweets t where  t.status_id=td.status_id)'''
    cur.execute(insert_sql)
    inserted_rows = cur.rowcount

    print("inserted row count = ",inserted_rows)

    conn.commit()
    cur.close()
    
def write_to_retweets(database ="project_db"):
    conn = connectSQLServer(database)
    cur = conn.cursor()

    insert_sql = '''INSERT INTO retweets
                                   (       retweet_status_id       ,
                                           retweet_src_status_id
                                    ) 
                                 select distinct rd.retweet_status_id       ,
                                            rd.retweet_src_status_id       
                       from retweets_delta rd where  not exists
                            (select 1 from retweets r where  r.retweet_status_id=rd.retweet_status_id and rd.retweet_src_status_id=r.retweet_src_status_id)'''
    cur.execute(insert_sql)
    inserted_rows = cur.rowcount

    print("inserted row count = ",inserted_rows)

    conn.commit()
    cur.close()
    
def write_to_quotes(database ="project_db"):
    conn = connectSQLServer(database)
    cur = conn.cursor()

    insert_sql = '''INSERT INTO quotes
                                   (       quote_status_id       ,
                                           quote_src_status_id
                                    ) 
                                 select distinct qd.quote_status_id       ,
                                            qd.quote_src_status_id       
                       from quotes_delta qd where  not exists
                            (select 1 from quotes q where  q.quote_status_id=qd.quote_status_id and qd.quote_src_status_id=q.quote_src_status_id)'''
    cur.execute(insert_sql)
    inserted_rows = cur.rowcount

    print("inserted row count = ",inserted_rows)

    conn.commit()
    cur.close()
