 package com.abhi

 import org.apache.spark.SparkConf
 import org.apache.spark.SparkContext
 import org.apache.spark.rdd.RDD
 import org.apache.spark.sql._
 import org.apache.spark.sql.types.{StringType, StructField, StructType}

 object HelloWorld {
   def main(args: Array[String]) : Unit = {
     val conf = new SparkConf().setAppName("IntegrationTestSparkJob")
     val sc = new SparkContext(conf)
     val sqlSc = new SQLContext(sc)
     val Array(inputPath, outputPath) = args
     // read the input file from input param 1
     val df = sqlSc.load(inputPath, "com.databricks.spark.avro")
     // write the count to input param2
     val schema = StructType(StructField("count", StringType, false) :: Nil)
     val outRdd = sc.parallelize(List(df.rdd.count().toString).map(c => Row(c)))
     val outDf = sqlSc.createDataFrame(outRdd, schema)
     outDf.save(outputPath, "com.databricks.spark.avro")
   }
 }