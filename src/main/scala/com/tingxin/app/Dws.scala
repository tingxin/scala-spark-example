package com.tingxin.app

import com.tingxin.entity.Item
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.SaveMode
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

object Dws extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .appName("com.tingxin")
    .getOrCreate()

  val targetTbName = "spark_yunji_order_dws"
  val basePath = "s3://tx-workshop/yunji/spark/yunji_order_dws/"

  val df = spark.sql(
    s"select logday,city,sex, spu, COUNT(order_id) as order_count,SUM(good_count) as good_count from yunji_order_dwd group by logday, city, sex, spu"
  )
  df.show(10)

  df.write
    .format("hudi")
    .options(getQuickstartWriteConfigs)
    .option(PRECOMBINE_FIELD.key(), "logday")
    .option(RECORDKEY_FIELD.key(), "logday")
    .option(PARTITIONPATH_FIELD.key(), "logday")
    .option(OPERATION.key(), "upsert")
    .option("hoodie.table.name", targetTbName)
    .option("hoodie.datasource.hive_sync.enable", "true")
    .option("hoodie.datasource.hive_sync.database", "default")
    .option("hoodie.datasource.hive_sync.table", targetTbName)
    .option("hoodie.datasource.hive_sync.mode", "HMS")
    .option("hoodie.datasource.hive_sync.use_jdbc", "false")
    .option("hoodie.datasource.hive_sync.username", "hadoop")
    .option("hoodie.datasource.hive_sync.partition_fields", "logday")
    .option(
      "hoodie.datasource.hive_sync.partition_extractor_class",
      "org.apache.hudi.hive.MultiPartKeysValueExtractor"
    )
    .mode(SaveMode.Append)
    .save(basePath)

  spark.sparkContext.stop()
}
