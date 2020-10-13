from pyspark.sql.types import *
from pyspark.sql import SparkSession
import logging
import schema
from spark_setup import run_spark
from postgresql_functions import write_to_db
from postgresql_functions import write_to_event_mentions
import time
def main():
    gdelt_start_time = time.time()

    spark,sqlctx = run_spark("mentions","9gb")
    
    mention_schema =  schema.get_event_mentions_schema()
    gdelt_file="s3a://gdelt-v2-data/event-mentions/202004*"
    df_1 = spark.read. \
            option("delimiter","\t"). \
            option("mode", "DROPMALFORMED"). \
            schema(mention_schema).csv(gdelt_file)

    df_2 = df_1.dropDuplicates()

    df_2.createOrReplaceTempView("event_mentions_vw")
    
    df_3 = spark.sql("SELECT global_event_id,mention_type,mention_source_name, \
    LOWER(mention_identifier) AS mention_identifier,actor1_char_offset,actor2_char_offset,action_char_offset, \
    confidence,mention_doc_tone \
                               FROM event_mentions_vw WHERE global_event_id IS NOT NULL")
    
    df_4=schema.transform_event_mentions_df(df_3)
    df_5 = df_4.dropDuplicates()

    table = "event_mentions_delta"
    mode = "overwrite"
    write_to_db(df_5, table, mode)
    write_to_event_mentions()

    gdelt_end_time = time.time()
    print("process_gdelt_mentions" ,gdelt_end_time-gdelt_start_time,"sec",(gdelt_end_time-gdelt_start_time)/60,"mins" )
    
    print("****************************************************")
    print("****************************************************")
    print("****************************************************")
    print()
    print("               Mentions                      ")
    print()

    print("****************************************************")
    print("****************************************************")
    print("****************************************************")



if __name__ == '__main__':
    main()
