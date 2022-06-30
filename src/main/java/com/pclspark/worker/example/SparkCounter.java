package com.pclspark.worker.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class SparkCounter {
  public static void process() {
    SparkConf conf = new SparkConf()
      .set("spark.cassandra.connection.host", "172.31.79.147")
      .set("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .set("spark.driver.host", "172.31.79.147")
      .setMaster("spark://172.31.79.147:7077")
      .setAppName("WORD_COUNTER");

    SparkSession spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate();

    JavaRDD<String> inputFile = spark.sparkContext()
      .textFile("s3a://gvcl-data/data/input/words.txt", 2)
      .toJavaRDD();

    JavaPairRDD<String, Integer> counts = inputFile
      .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
      .mapToPair(word -> new Tuple2<>(word, 1))
      .reduceByKey((a, b) -> a + b);

    counts.saveAsTextFile("s3a://gvcl-data/data/output/wordcount");
  }
}