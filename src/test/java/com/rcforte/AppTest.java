package com.rcforte;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class AppTest {

  @Test
  @Ignore
  public void shouldAnswerWithTrue() {
    List<Double> inputData = Lists.newArrayList(1.0, 2.0, 3.0);
    SparkConf conf = new SparkConf()
        .setAppName("Java Test App")
        .setMaster("local[*]");
    JavaSparkContext ctx = new JavaSparkContext(conf);
    JavaRDD<Double> rdd = ctx.parallelize(inputData);
    System.out.println(rdd.count());
    assertTrue(true);
  }

  @Test
  public void shouldUseKafkaStructuredStreaming() throws Exception {
    StructType schema = DataTypes.createStructType(
        new StructField[]{
            DataTypes.createStructField("symbol", DataTypes.StringType, false),
            DataTypes.createStructField("price", DataTypes.DoubleType, false),
            DataTypes.createStructField("date", DataTypes.StringType, false)
        }
    );
    SparkSession spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("kafka-pipeline")
        .getOrCreate();
    // Read raw message from kafka
    Dataset<Row> inputDF = spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "PricesInput")
        .load();
    inputDF.printSchema();

    // Extract value and convert to json
    Dataset<Row> jsonStrDF = inputDF
        // convert to string
        .selectExpr("cast(value as string) as message")
        // convert to json
        .select(functions.from_json(functions.col("message"), schema).as("json"));

    // Convert to object
    Dataset<Price> pricesDF = jsonStrDF
        // select all json fields
        .select("json.*")
        // encode it as Price obj
        .as(Encoders.bean(Price.class));
    pricesDF.createOrReplaceTempView("prices");

    //Dataset<Row> pricesDF = spark.sql("select symbol, date, price from prices");
    //pricesDF.printSchema();
    //if(false) {
    //StreamingQuery console = pricesDF
    //.writeStream()
    //.format("console")
    //.outputMode(OutputMode.Append())
    //.start();
    //console.awaitTermination();
    //}

    StreamingQuery kafka = pricesDF
        //.select(functions.to_json(functions.col("price")))
        .writeStream()
        .format("kafka")
        .option("checkpointLocation", "/home/rcforte/kafka-checkpoint-location")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "PricesOutput")
        .foreach(new PriceForEachWriter())
        .start();
    kafka.awaitTermination();
  }

  @Test
  public void shouldUseKafkaStructuredStreaming_1() throws Exception {
    StructType schema = DataTypes.createStructType(
        new StructField[]{
            DataTypes.createStructField("symbol", DataTypes.StringType, false),
            DataTypes.createStructField("price", DataTypes.DoubleType, false),
            DataTypes.createStructField("date", DataTypes.StringType, false)
        }
    );
    SparkSession spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("kafka-pipeline")
        .getOrCreate();

    spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "PricesInput")
        .load()
        .selectExpr("cast(value as string) as payload")
        .select(functions.from_json(functions.col("payload"), schema).as("json"))
        .select("json.*").as(Encoders.bean(Price.class))
        .writeStream()
        .format("kafka")
        .option("checkpointLocation", "/home/rcforte/kafka-checkpoint-location")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "PricesOutput")
        .foreach(new PriceForEachWriter())
        .start()
        .awaitTermination();
  }
}

