package com.tingxin.app

import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.SaveMode
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._


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

  val df = spark.sql(
    s"select order_id,user_mail,good_count,status,create_time,logday from $sourceTbName where logday > '2022-03-20'"
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

  if (verificationResult.status == CheckStatus.Success) {
    println("The data passed the test, everything is fine!")
  } else {
    println("Dws data quality:\n")

    val resultsForAllConstraints = verificationResult.checkResults
      .flatMap { case (_, checkResult) => checkResult.constraintResults }

    resultsForAllConstraints
      .filter { _.status != ConstraintStatus.Success }
      .foreach { result =>
        println(s"${result.constraint}: ${result.message.get}")
      }
  }

  df.write.format("hudi")
    .options(getQuickstartWriteConfigs)
    .option(PRECOMBINE_FIELD.key(), "ts")
    .option(RECORDKEY_FIELD.key(), "order_id")
    .option(PARTITIONPATH_FIELD.key(), "logday")
    .option(OPERATION.key(), "upsert")
    .option(DataSourceWriteOptions.TABLE_NAME.key(), targetTbName)
    .option("hoodie.datasource.hive_sync.enable", "true")
    .option("hoodie.datasource.hive_sync.database","default")
    .option("hoodie.datasource.hive_sync.table", targetTbName)
    .option("hoodie.datasource.hive_sync.mode","HMS")
    .option("hoodie.datasource.hive_sync.use_jdbc","false")
    .option("hoodie.datasource.hive_sync.username","hadoop")
    .option("hoodie.datasource.hive_sync.partition_fields", "logday,hh")
    .option("hoodie.datasource.hive_sync.partition_extractor_class","org.apache.hudi.hive.MultiPartKeysValueExtractor")
    .mode(SaveMode.Append)
    .save(basePath)

  spark.sparkContext.stop()
}
