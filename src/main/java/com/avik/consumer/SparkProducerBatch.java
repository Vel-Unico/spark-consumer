package com.avik.consumer;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaHadoopRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkProducerBatch {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "F:\\avik\\winutils");
		SparkSession ss = SparkSession.builder().master("local[2]").appName("SparkConsumer").getOrCreate();
		
		
		Dataset<Row> df = ss.read().format("kafka")
				  .option("kafka.bootstrap.servers", "192.168.204.129:9092")
				  .option("subscribe", "kafka")
				  .load();
		
		Dataset<Row> data=ss.read().format("csv").load("F:\\avik\\spark-streaming-kafka\\src\\main\\resourcces\\data.txt");
		
		data.show();
		
		//df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show();
					data
				  .selectExpr("CAST(key  AS STRING)", "CAST(value AS STRING)")
				  .write()
				  .format("kafka")
				  .option("kafka.bootstrap.servers", "192.168.204.129:9092")
				  .option("topic", "kafka")
				  .save();
	}


}
