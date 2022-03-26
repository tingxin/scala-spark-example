package com.tingxin.app

import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.SaveMode
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Date

/** Computes an approximation to pi */
object Dwd extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .appName("com.tingxin")
    .getOrCreate()

  val sourceTbName = "flink_hudi_order_ods"
  val targetTbName = "spark_hudi_order_dwd"
  val basePath = "s3://tx-workshop/rongbai/spark/hudi_order_dwd1/"
  val qualityBasePath = "s3://tx-workshop/rongbai/spark/quality_stat/"

  val df = spark.sql(
    s"select order_id,user_mail,good_count,status,create_time,ts,logday from $sourceTbName where logday > '2022-03-20'"
  )
  df.show(10)

  val verificationResult = VerificationSuite()
    .onData(df)
    .addCheck(
      Check(CheckLevel.Error, "unit testing my data")
        .hasSize(_ > 10) // we expect 5 rows
        .isComplete("order_id") // should never be NULL
        .isUnique("order_id") // should not contain duplicates
        .isComplete("user_mail") // should never be NULL
        // should only contain the values "high" and "low"
        .isContainedIn("status", Array("unpaid", "paid", "cancel", "shipping"))
        .isNonNegative("good_count") // should not contain negative values
        // at least half of the descriptions should contain a url
        // half of the items should have less than 10 views
        .hasApproxQuantile("good_count", 0.5, _ <= 10)
    )
    .run()

  var resultDf = checkResultsAsDataFrame(spark, verificationResult)

  val pattern = "yyyy-MM-dd HH:mm:00"
  val date = new Date()
  val sdf = new SimpleDateFormat(pattern)
  val timeTip = sdf.format(date)

  resultDf = resultDf.withColumn("taskName", lit("validate order dwd"))
  resultDf = resultDf.withColumn("validateTime", lit(timeTip))

  val qualityDf = resultDf.select(
    "taskName",
    "validateTime",
    "constraint",
    "constraint_status",
    "constraint_message"
  )

  qualityDf.write
    .format("parquet")
    .option("path", qualityBasePath)
    .mode("append")
    .saveAsTable("default.quality_stat")

  df.write
    .format("hudi")
    .options(getQuickstartWriteConfigs)
    .option(PRECOMBINE_FIELD.key(), "ts")
    .option(RECORDKEY_FIELD.key(), "order_id")
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
