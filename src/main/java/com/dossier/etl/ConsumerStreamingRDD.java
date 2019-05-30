package com.dossier.etl;

import java.sql.Time;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

public class ConsumerStreamingRDD {


	private static final Logger LOGGER=Logger.getLogger(SparkConsumerStreaming.class);
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		
		System.setProperty("hadoop.home.dir", "F:\\avik\\winutils");
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkConsumerStreaming");
		SparkContext sc =new SparkContext(conf);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", "192.168.190.128:9092");
		kafkaParams.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		kafkaParams.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		kafkaParams.put("group.id", "test-consumer-group");
		Collection<String> topics = Arrays.asList("kafka");
		
		
		
		OffsetRange[] offRanges = {
				OffsetRange.create("test", 0, 50, 100),OffsetRange.create("test", 0, 60,90)
		};
		
		JavaRDD inputRDD =
				  KafkaUtils.createRDD(jsc, kafkaParams, offRanges, LocationStrategies.PreferConsistent());
				  
		
		
		
	}
	

}
