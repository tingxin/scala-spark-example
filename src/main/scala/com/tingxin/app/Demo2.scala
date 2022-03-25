package com.tingxin.app
import scala.math.random
import org.apache.spark._
import com.tingxin.entity.Item
import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi */
object Demo2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
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

    val data = spark.createDataFrame(rdd)

    data.show(10)

    spark.sparkContext.stop()
  }
}
