from pyspark.sql.types import *
from pyspark.sql import SparkSession
import logging
import schema
from spark_setup import run_spark
from postgresql_functions import write_to_db
from postgresql_functions import write_to_events
import time
def main():
    gdelt_start_time = time.time()

    spark,sqlctx = run_spark("events","9gb")
    
    gdelt_file="s3a://gdelt-v2-data/events/202004*"
    events_schema =  schema.get_events_schema()
    df_1 = spark.read. \
            option("delimiter","\t"). \
            option("mode", "DROPMALFORMED"). \
            schema(events_schema).csv(gdelt_file)

    df_2 = df_1.dropDuplicates()
    
    df_2.createOrReplaceTempView("events_vw")
    df_3 = spark.sql("SELECT * FROM events_vw WHERE global_event_id IS NOT NULL")
    
    df_4 = schema.transform_events_df(df_3)
    df_5 = df_4.dropDuplicates()

    table = "events_delta"
    mode = "overwrite"
    write_to_db(df_5,table, mode)
    write_to_events()
    gdelt_end_time = time.time()
    print("process_gdelt_events" ,gdelt_end_time-gdelt_start_time )

    print("****************************************************")
    print("****************************************************")
    print("****************************************************")
    print()
    print("               Events                      ")
    print()
    print("****************************************************")
    print("****************************************************")
    print("****************************************************")





if __name__ == '__main__':
    main()
