spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,com.databricks:spark-xml_2.11:0.6.0,org.postgresql:postgresql:42.2.16 \
--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.hadoop.fs.s3a.endpoint=s3.us-west-1.amazonaws.com \
--executor-memory 100G --total-executor-cores 8 \
--master spark://ip-10-0-0-4:7077 process_gdelt_events.py
spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,com.databricks:spark-xml_2.11:0.6.0,org.postgresql:postgresql:42.2.16 \
--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.hadoop.fs.s3a.endpoint=s3.us-west-1.amazonaws.com \
--executor-memory 100G --total-executor-cores 8 \
--master spark://ip-10-0-0-4:7077 process_gdelt_mentions.py
spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,com.databricks:spark-xml_2.11:0.6.0,org.postgresql:postgresql:42.2.16 \
--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.hadoop.fs.s3a.endpoint=s3.us-west-1.amazonaws.com \
--executor-memory 100G --total-executor-cores 8 \
--master spark://ip-10-0-0-4:7077 process_gdelt_twitter.py
