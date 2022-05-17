package com.tingxin.app
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.functions._

import scala.math.random
import org.apache.spark._
import com.tingxin.entity.Item
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date
import scala.reflect.internal.util.TableDef.Column

/** Computes an approximation to pi */
object Demo3 extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .config("spark.driver.host", "192.168.1.8")
    .appName("com.tingxin")
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(
    Seq(
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12)
    )
  )

  val df = spark.createDataFrame(rdd)
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
  val pattern = "yyyy-MM-dd HH:mm"
  val date = new Date()
  val sdf = new SimpleDateFormat(pattern)
  val timeTip = sdf.format(date)
  var resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
  resultDataFrame = resultDataFrame.withColumn("taskName",lit("validate order dwd"))
  resultDataFrame = resultDataFrame.withColumn("validateTime",lit(timeTip))
  val focusDf = resultDataFrame.select("taskName","validateTime","constraint", "constraint_status", "constraint_message")
  focusDf.show()



  spark.sparkContext.stop()
}
