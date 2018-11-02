package com.naresh

import java.util.concurrent.TimeUnit

import com.databricks.spark.avro._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.avro.generic.GenericData.StringType
import java.util.Calendar
import java.util.function.ToDoubleFunction
import org.apache.spark.sql.types.DataTypes
import java.sql.Date
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Spark_S3_Integration {

	def main(args: Array[String]): Unit = {

			println("---  Scala-S3-Integration -- Started ---")

			// Set the log level to only print errors
			Logger.getLogger("org").setLevel(Level.ERROR)

			// Create a SparkContext using every core of the local machine, named RatingsCounter
			val sc = new SparkContext("local[*]", "Scala-S3-Integration")
		   
			println("---  sqlContext Loading... ");
			val sqlContext = new org.apache.spark.sql.SQLContext(sc);
			
			println("---  Setting up start time ---")
			val avroStart = System.nanoTime();
			
			println("---  Reading Avro data from HDFS ---")
			//val df = sqlContext.read.format("com.databricks.spark.avro").load("hdfs://10.86.24.17:9000/spark/users.avro")
			val df = sqlContext.read.format("com.databricks.spark.avro").load("./users.avro")
			
			//Creating Temp table in Spark
			df.createTempView("temp_events");
			
			//Fetching the Temp records with timestamp
			val tmpRecords = sqlContext.sql("SELECT cast(eventDate/1000 as timestamp) eventTime,c.* from temp_events c");
			 
			//removing duplicates
			val distRecords = tmpRecords.dropDuplicates()
		 
			distRecords.repartition(1).write
			  .format("com.databricks.spark.csv")
			  .mode(SaveMode.Overwrite)
			  .option("header", true)
			  .save("scala_s3_output.csv");
		 
			println("---  Setting up end time ---")
			val avroEnd = System.nanoTime();
			time(avroEnd, avroStart);  
		 	
			println("---  df show ")
			distRecords.show() 
			 
	}
	
	def time(time2:Long, time1:Long): Unit = {
			val leng = TimeUnit.NANOSECONDS.toSeconds(time2 - time1);
			println()
			println("------- Time Taken ----------")
			println("---  Finished toNanos   : " + (time2 - time1) + " ns")
			println("---  Finished toSeconds : " + leng + " secs")
			println("---  Finished toMinutes : " + TimeUnit.SECONDS.toMinutes(leng) + " mins")
			println("------------------------------")
	}
}