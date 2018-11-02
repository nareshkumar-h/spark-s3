package com.naresh;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.dayofmonth;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.year;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Application {
	
 
	public static void main(String[] args) {

		System.out.println("Create Spark Session");
		SparkSession spark = SparkSession.builder().master("local").getOrCreate();

		//String destinationDir = "D://tmp//output/";
		String destinationDir = "s3_output/";
		// String destinationDir = "s3a://db-events-output/user_ps/";

		// String files = "d://tmp//input//";
		String files = "local_s3//users.avro";
		System.out.println("Read DataFrames");
		// Creates a DataFrame from a specified file

		Dataset<Row> df = spark.sqlContext().read().format("com.databricks.spark.avro").load(files);
		df.createOrReplaceTempView("temp_events");
		Dataset<Row> sqlRows = df.sqlContext()
				.sql("SELECT cast(eventDate/1000 as timestamp) eventTime,c.* from temp_events c");
		sqlRows.show();

		Dataset<Row> rows = sqlRows.withColumn("year", year(col("eventTime")))
				.withColumn("month", month(col("eventTime"))).withColumn("day", dayofmonth(col("eventTime")))
				.withColumn("hour", hour(col("eventTime")));

		long rawCount = df.count();
		rows.show();
		Dataset<Row> distinctRows = rows.distinct();
		distinctRows.show();
		long distinctCount = distinctRows.count();
		System.out.println("RawCount:" + rawCount + ",dedupDistCount:" + distinctCount);

		write(destinationDir, distinctRows);

		spark.close();

	}

	private static void write(String destinationDir, Dataset<Row> distinctRows) {

		distinctRows.repartition(1).write()
				// .format("parquet")
				.mode(SaveMode.Overwrite).option("header", true).format("csv")
				.partitionBy("year", "month", "day", "hour").save(destinationDir);
		// df.write().mode(SaveMode.Append).format("com.databricks.spark.avro").save(destinationDir);
	}

}
