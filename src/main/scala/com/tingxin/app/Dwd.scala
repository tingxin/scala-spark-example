package com.tingxin.app

import org.apache.spark._
import com.tingxin.entity.Item
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus

/** Computes an approximation to pi */
object Dwd extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .appName("com.tingxin")
    .getOrCreate()

  val sourceTbName = "flink_hudi_order"
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

  val hudiOptions = Map(
    "hoodie.table.name" -> targetTbName,
    "hoodie.datasource.write.recordkey.field"->  "order_id",
    "hoodie.datasource.write.partitionpath.field"->  "ts",
    "hoodie.datasource.write.table.name"->  targetTbName,
    "hoodie.datasource.write.operation"->  "upsert",
    "hoodie.datasource.write.precombine.field"->  "ts",
    "hoodie.upsert.shuffle.parallelism"->  2
  )

  df.write.format("hudi")
    .option("hoodie.table.name", targetTbName)
    .option("hoodie.datasource.write.recordkey.field", "order_id")
    .option("hoodie.datasource.write.partitionpath.field", "ts")
    .option("hoodie.datasource.write.table.name", targetTbName)
    .option("hoodie.datasource.write.operation",  "upsert")
    .option("hoodie.datasource.write.precombine.field", "ts")
    .option("hoodie.upsert.shuffle.parallelism", 2)
    .mode("overwrite")
    .save(basePath)

  spark.sparkContext.stop()
}
