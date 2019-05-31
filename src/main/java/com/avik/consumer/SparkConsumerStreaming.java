package com.avik.consumer;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

public class SparkConsumerStreaming {

    private static final Logger LOGGER = Logger.getLogger(SparkConsumerStreaming.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        // System.setProperty("hadoop.home.dir", "F:\\avik\\winutils");
        SparkSession ss = SparkSession.builder().getOrCreate();

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkConsumerStreaming");
        Configuration hconf = new Configuration();
        FileSystem fs = FileSystem.get(hconf);
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(Integer.valueOf(args[1])));

        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        ProducerProperties producerProperties = new ProducerProperties();
        Map<String, String> property = producerProperties.getPropValues();
        kafkaParams.put("bootstrap.servers", property.get("BOOTSTRAP-SERVERS"));
        kafkaParams.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
        kafkaParams.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
        kafkaParams.put("group.id", "test-consumer-group");
        // kafkaParams.put("auto.offset.reset", args[2]);

        Collection<String> topics = Arrays.asList(args[0]);// args[0] topic name

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        System.out.println("start1");

        // String path = "output/file-";
        String path = "user/hadoop/test/";
        DateTimeFormatter dft = DateTimeFormatter.ofPattern("HH-mm-ss");

        stream.foreachRDD(rdd -> {
            rdd.foreach(line -> {
                System.out.println(line.value());
                sendMessage(args[0], line.value());
            });

            LocalDateTime now = LocalDateTime.now();
            System.out.println(path + dft.format(now));

            JavaRDD jrdd = rdd.map(new Function<ConsumerRecord<String, String>, Row>() {
                @Override
                public Row call(ConsumerRecord<String, String> line) throws Exception {
                    return RowFactory.create(line.value());
                }
            });
            // String _name = dft.format(now);
            // jrdd.saveAsTextFile(path + _name);
            // Long size=fs.getFileStatus(new Path(_name)).getLen();
            // System.out.println("Size of the data in bytes = "+size);
        });

        jsc.start();
        jsc.awaitTermination();
    }

    public static void sendMessage(String topic, String message) throws IOException {
        System.out.println("TOPIC - " + topic + "\n" + "MESSAGE - " + message);
        URL obj = new URL("http://52.170.112.45:4000/publish/" + topic + "/" + URLEncoder.encode(message, "UTF-8"));
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", "Mozilla/5.0");
        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            System.out.println("TOPIC - " + topic + "\n" + "MESSAGE - " + message);
        } else {
            System.out.println("GET request not worked");
        }
    }

}