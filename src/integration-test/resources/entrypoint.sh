hdfs dfsadmin -safemode leave
hdfs dfs -mkdir -p /input
hdfs dfs -put /twitter.avro /input/twitter.avro
hdfs dfs -ls /input
spark-submit --class com.abhi.HelloWorld --master local[1] /SparkIntegrationTestsAssembly.jar /input/twitter.avro /output