package com.pclspark.worker;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.pclspark.config.SparkContextBuilder;
import com.pclspark.model.ProcessedRawPoint;
import com.pclspark.model.RawPoint;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.pclspark.config.Utility.getArgumentMap;

public class SpacePartitioner {

  public static void main(String args[]) {
    process(getArgumentMap(args));
  }

  private static void process(Map<String, String> params) {
    SparkContext sparkContext = SparkContextBuilder
        .buildSparkCassandraConnectorContext();

    CassandraTableScanJavaRDD<RawPoint> pointTable = CassandraJavaUtil
      .javaFunctions(sparkContext)
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
