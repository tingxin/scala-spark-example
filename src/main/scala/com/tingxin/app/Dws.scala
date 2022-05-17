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

  val sourceTbName = "yunji_order_dwd"
  val targetTbName = "spark_yunji_order_dws"
  val basePath = "s3://tx-workshop/yunji/spark/yunji_order_dws/"

  val df = spark.sql(
    s"select COUNT(order_id) as order_count," +
      s"COUNT(DISTINCT user_mail) as user_count," +
      s"SUM(good_count) as good_count," +
      s"MAX(ts) as ts," +
      s"logday from $sourceTbName group by logday"
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
