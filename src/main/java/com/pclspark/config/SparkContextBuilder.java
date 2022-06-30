package com.pclspark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class SparkContextBuilder {

  public static SparkContext buildSparkCassandraConnectorContext() {
    SparkContextBuilder sparkContextBuilder = new SparkContextBuilder();

    Properties properties = sparkContextBuilder
        .getConfiguration();

    SparkConf conf = new SparkConf()
      .set("spark.cassandra.connection.host",
        properties.getProperty("spark.cassandra.connection.host"))
      .set("spark.hadoop.fs.s3a.aws.credentials.provider",
        properties.getProperty("spark.hadoop.fs.s3a.aws.credentials.provider"))
      .set("spark.driver.host",
        properties.getProperty("spark.driver.host"))
      .set("spark.driver.bindAddres",
        properties.getProperty("spark.driver.bindAddres"))
      .setMaster(properties.getProperty("spark.master"))
      .setAppName(properties.getProperty("spark.app.name"));

    SparkSession spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate();

    SparkContext context = spark.sparkContext();

    return context;
  }

  private Properties getConfiguration() {

    String resourceName = "/config.properties";
    String absolutePath = getClass()
      .getResource("").getFile()
      .replace("file:", "")
      .replace("!", "")
      .replace(".jar", "");

    Properties prop = new Properties();

    FileInputStream propsInput = null;
    try {
      propsInput = new FileInputStream(absolutePath);
      prop.load(propsInput);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return prop;
  }
}
