package org.example

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.checks.{Check, CheckLevel}

/**
 * Test IO to wasb
 */
object MyDeequChecks {
  def main (arg: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("MyDeequChecks").master("local[*]").getOrCreate()

    /*
    case class dataSchema(marketplace: String, customer_id: String, review_id: String, product_id: String,
    product_parent: String, product_title: String, star_rating: Int, helpful_votes: Int,
    total_votes: Int, vine: String, verified_purchase: String, review_headline: String,
    review_body: String, review_date: BigInt, year: Int)
    */

    val dataset = spark.read.format("tsv").option("header", "true").option("delimiter", "\t").

      load("\"C:\\\\Users\\\\kira_podlesnaia\\\\Desktop\\\\big_data_course\\\\QualityChecksDequee\\\\amazon_reviews_us_Camera_v1_00.tsv\\\\amazon_reviews_us_Camera_v1_00.tsv\"")

    val verificationResult: VerificationResult = { VerificationSuite()
      // data to run the verification on
      .onData(dataset)
      // define a data quality check
      .addCheck(
        Check(CheckLevel.Error, "Review Check")
          // .hasSize(_ >= 3000000) // at least 3 million rows
          .isContainedIn("verified_purchase", Array("X", "Y"))
          .hasPattern("review_date", """([0-9]{4}-[0-9]{2}-[0-9]{2})""".r)
          .hasDataType("total_votes", ConstrainableDataTypes.Numeric )
          .isComplete("review_id ") // should never be NULL
          .isUnique("review_id")) // should not contain duplicates
      .run()
    }

    // convert check results to a Spark data frame
    val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
    resultDataFrame.show(truncate=false)
    spark.stop()

  }
}

