package com.pclspark.worker.example;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.pclspark.model.ProcessedRawPoint;
import com.pclspark.model.RawPoint;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class SparkCassandra {

  public static void process(Map<String, String> parameters) {

    SparkConf conf = new SparkConf()
      .set("spark.cassandra.connection.host", "172.31.79.147")
      .set("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .set("spark.driver.host", "172.31.79.147")
      .set("spark.driver.bindAddres", "172.31.79.147")
      .setMaster("spark://172.31.79.147:7077")
      .setAppName("WORD_COUNTER");

    SparkSession spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate();

    SparkContext context = spark.sparkContext();

    CassandraTableScanJavaRDD<RawPoint> pointTable = CassandraJavaUtil
      .javaFunctions(context)
      .cassandraTable(
        "SampledPoints",
        "points",
        mapRowTo(RawPoint.class)
      );

    CassandraTableScanJavaRDD<RawPoint> pointTableRdd = pointTable
      .select("tileid", "x", "y", "z", "c");

    JavaRDD<ProcessedRawPoint> processedRawPointJavaRDD = pointTableRdd
      .mapPartitions(new FlatMapFunction<Iterator<RawPoint>, ProcessedRawPoint>() {
        @Override
        public Iterator<ProcessedRawPoint> call(Iterator<RawPoint> rawPointIterator) throws Exception {

          List<ProcessedRawPoint> processedRawPoints = new ArrayList<>();
          Random ran = new Random();

          while(rawPointIterator.hasNext()) {
            RawPoint rawPoint = rawPointIterator.next();
            ProcessedRawPoint processedRawPoint = new ProcessedRawPoint();
            processedRawPoint.tileid =rawPoint.tileid;
            processedRawPoint.x = rawPoint.x;
            processedRawPoint.y = rawPoint.y;
            processedRawPoint.z = rawPoint.z;
            processedRawPoint.c = rawPoint.c;
            processedRawPoint.regionid = ran.nextInt(5) + 1;

            processedRawPoints.add(processedRawPoint);
          }

          return processedRawPoints.iterator();
        }
    });

    CassandraJavaUtil
      .javaFunctions(processedRawPointJavaRDD)
      .writerBuilder("SampledPoints", "processedrawpoint", mapToRow(ProcessedRawPoint.class))
      .saveToCassandra();
  }
}