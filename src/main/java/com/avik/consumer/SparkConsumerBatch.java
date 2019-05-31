package com.avik.consumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Array;

public class SparkConsumerBatch {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "F:\\avik\\winutils");
		SparkSession ss = SparkSession.builder().master("local[2]").appName("SparkConsumer").getOrCreate();
		Dataset<Row> df = ss.read().format("kafka")
				  .option("kafka.bootstrap.servers", "192.168.204.129:9092")
				  .option("subscribe", "kafka")
				  .load();
		
		//df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show();
		
		JavaRDD data = df.selectExpr( "CAST(value AS STRING)").toJavaRDD().map(new Function<Row, Row>() {

			public Row call(Row line) throws Exception {
				String[] split = line.mkString().split(",");
				Row newRow=RowFactory.create(split[0],split[1],split[2],split[3]);
				return newRow;
			}
		});
		List<StructField> schemaList = new ArrayList<StructField>();
		String Schema = "order_id,order_date,country,order_value";
		for (String field : Schema.split(",")) {
			schemaList.add(DataTypes.createStructField(field, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(schemaList);
		Dataset<Row> res=ss.sqlContext().createDataFrame(data, schema);
		res.show();
	}
}
