package com.tingxin.app

import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Date

/** Computes an approximation to pi */
object CheckDws extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .appName("com.tingxin")
    .getOrCreate()

  val sourceTbName = "spark_yunji_order_dws"

  val qualityBasePath = "s3://tx-workshop/rongbai/spark/quality_stat/"

  val df = spark.sql(s"select * from $sourceTbName")
  df.show(10)

  val verificationResult = VerificationSuite()
    .onData(df)
    .addCheck(
      Check(CheckLevel.Error, "unit testing my data")
        .hasSize(_ > 10) // we expect 5 rows
        .isComplete("logday") // should never be NULL
        .hasMin("order_count",  x=> x == 10)
        .hasMax("order_count", x=> x == 10000)
        .hasMin("good_count", x=> x == 10)
        .hasMax("good_count", x=> x == 100)
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
    .saveAsTable("default.yunji_quality_stat")

  spark.sparkContext.stop()
}
