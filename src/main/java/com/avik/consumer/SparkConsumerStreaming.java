package com.avik.consumer;

import java.io.DataOutputStream;
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
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.json.simple.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkConsumerStreaming {

    private static final Logger LOGGER = Logger.getLogger(SparkConsumerStreaming.class);
    private static SparkSession ss;
    public static void main(String[] args) throws InterruptedException, IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        ProducerProperties producerProperties = new ProducerProperties();
        Map<String, String> property = producerProperties.getPropValues();

        // System.setProperty("hadoop.home.dir", "D:\\avik\\winutils");
        
        ss=SparkSession.builder().getOrCreate();
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkConsumerStreaming");
        //ss= SparkSession.builder().config(conf).getOrCreate();
        Configuration hconf = new Configuration();
        FileSystem fs = FileSystem.get(hconf);
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(Integer.valueOf(args[1])));

        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "BOOTSTRAP-SERVERS");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "test-consumer-group");
        // kafkaParams.put("auto.offset.reset", args[2]);

       Collection<String> topics = Arrays.asList(args[0]);// args[0] topic name
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        System.out.println("start1");

        // String path = "output/file-";
        String path = "/user/hadoop/test/";
        DateTimeFormatter dft = DateTimeFormatter.ofPattern("HH-mm-ss");

        stream.foreachRDD(rdd -> {
            rdd.foreach(line -> {
                System.out.println(line.value());
                sendMessage(args[2], args[0], line.value());
            });

            LocalDateTime now = LocalDateTime.now();
            System.out.println(path + dft.format(now));

            JavaRDD jrdd = rdd.map(new Function<ConsumerRecord<String, String>, Row>() {
                @Override
                public Row call(ConsumerRecord<String, String> line) throws Exception {
                    return RowFactory.create(line.value());
                }
            });
            String _name = dft.format(now);
            String fullPath=path + _name;
            jrdd.saveAsTextFile(path + _name);
           // createBlobAccount(jrdd,_name);
            // Long size=fs.getFileStatus(new Path(_name)).getLen();
            // System.out.println("Size of the data in bytes = "+size);
        });

        jsc.start();
        jsc.awaitTermination();
    }

    public static void createBlobAccount(JavaRDD jrdd, String _name) {
    	String accessKey = "AKIA3F7XRLHHEKE7TZWY";
    	String secretId="7cZ2RZrzatcnmRxZD7cs+kz66EoiSuktLIR+Cp+V";
    	String bucketName="testawsavik";
    	String folderName=_name;
    	String accountName="blobconnectors";
		String accountKey="p5AS3NlUWATnh4oGbeuhBpYLHPEuTsWwMIX0aVh/M0KwiTFPPTRbvtWgi5XdG8xBnLXpYTzE6CrAyfE20o+G5Q==";
		String containerName = "blobcontainer";
		ss.sparkContext().hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		String hdfsDirPath = ("hdfs://localhost"+ _name);
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/etc/hadoop/hdfs-site.xml"));
		conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		Path location = new Path(hdfsDirPath); 
		//ss.sparkContext().conf().set("fs.azure.account.key."+accountName+".blob.core.windows.net", accountKey);
    	//jrdd.saveAsTextFile("wasbs://"+containerName+"@"+accountName+".blob.core.windows.net/"+folderName);
		jrdd.saveAsTextFile("s3n://"+accessKey+":"+secretId+"@"+bucketName+"/"+folderName);
		//sourceData.write().option("header", "true").format("CSV").mode(SaveMode.Overwrite)
		//.save("s3n://"+accessKey+":"+secretId+"@"+bucketName+"/"+folderName); 
    	//sourceData.write().option("header", "true").format("CSV").mode(SaveMode.Overwrite).save("wasbs://"+containerName+"@"+accountName+".blob.core.windows.net/"+folderName);    	
	}

	public static void sendMessage(String baseUrl, String topic, String message) throws IOException {
        System.out.println("TOPIC - " + topic + "\n" + "MESSAGE - " + message);
        URL obj = new URL(baseUrl + "/send");
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        con.setDoOutput(true);
        con.setDoInput(true);
        con.setRequestProperty("User-Agent", "Mozilla/5.0");
        JSONObject body = new JSONObject();
        body.put("topicName", topic);
        body.put("message", message);
        String postJsonData = body.toString();
        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(postJsonData);
        wr.flush();
        wr.close();

        int responseCode = con.getResponseCode();

        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            System.out.println("TOPIC - " + topic + "\n" + "MESSAGE - " + message);
        } else {
            System.out.println("GET request not worked");
        }
    }

}