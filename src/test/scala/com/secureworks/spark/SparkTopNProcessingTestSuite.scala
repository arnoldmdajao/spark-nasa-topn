package com.secureworks.spark

import java.sql.Date

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.secureworks.spark.SparkTopNProcessing._
import com.secureworks.spark.builders.rankedBuilder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class SparkTopNProcessingTestSuite extends FunSuite  with DataFrameSuiteBase {
// Set logging to error level only for easy Development
  Logger.getLogger("org").setLevel(Level.ERROR)

//  Initialized spark session for all test
//  val spark: SparkSession = SparkSession.builder.master("local[*]").getOrCreate()
  import spark.implicits._

//  Initialize variables to be used in the test
  val sourceFile = "/data/TestData.txt"
  val dailyRankCount = 3 // Top N = Top 3


  test("testSourceToDataFrame should read source file and return DataFrame") {

    // Fetch dataframe from sourcefile
    val sourceDF = sourceToDataFrame(sourceFile, spark)

    // Simple DataFrame Count Check
    assert(sourceDF.count === 500)

    //Check if DataFrame can be read using header
    val header = """[199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245]"""
    assert(sourceDF.head().toString() === header)
  }

  test("parseLogsDataFrame should return parsed DataFrame from source DataFrame") {

    val sourceDF = sourceToDataFrame(sourceFile, spark)
    val parsedDF = parseLogsDataFrame(sourceDF, spark)

    // Simple DataFrame Count Check
    assert(sourceDF.count === 500)
    assert(parsedDF.count === 500)

  }

  test("getDiscardedDataFrame should return 1 DataFrame that does not confirm to Pattern") {
    // Last row of data is incomplete
    val sourceDF = sourceToDataFrame(sourceFile, spark)
    val parsedDF = parseLogsDataFrame(sourceDF, spark)
    val discardedDF = getDiscardedDataFrame(parsedDF, spark)
    // Simple DataFrame Count Check
    assert(sourceDF.count === 500)
    assert(parsedDF.count === 500)
    assert(discardedDF.count === 1)
  }

  test("getRankedDataFrame should return 1 proper ranking of based on Column GroupBy Keys") {
    // Last row of data is incomplete
    val sourceDF = sourceToDataFrame(sourceFile, spark)
    val parsedDF = parseLogsDataFrame(sourceDF, spark)
    // Simple DataFrame Count Check
    assert(sourceDF.count === 500)
    assert(parsedDF.count === 500)

    //common config
    val col_group_keys = Seq("log_date", "ip_address", "log_uri")
    val TopN_number = 3
    // Simple DataFrame Count Check
    // using dense_rank
    val dense_rankedDF = getRankedDataFrame(parsedDF,TopN_number,col_group_keys,spark=spark)
    val dense_rankedDayDF = dense_rankedDF.filter($"log_date" === lit("1995-07-06"))
    dense_rankedDayDF.show(false)
    assert(dense_rankedDayDF.count === 6)
    // using rank
    val rankedDF = getRankedDataFrame(parsedDF,TopN_number,col_group_keys,false,spark=spark)
    val rankedDayDF = rankedDF.filter($"log_date" === lit("1995-07-06"))
    rankedDayDF.show(100,false)
    assert(rankedDayDF.count === 4)
  }

  test("getRankedDataFrame should return proper ranking with only log_date column and ip_address ") {
    // Last row of data is incomplete
    val sourceDF = sourceToDataFrame(sourceFile, spark)
    val parsedDF = parseLogsDataFrame(sourceDF, spark)
    // Simple DataFrame Count Check
    assert(sourceDF.count === 500)
    assert(parsedDF.count === 500)

    //common config
    val col_group_keys = Seq("log_date","ip_address")
    val TopN_number = 4
    // Simple DataFrame Count Check
    // using dense_rank
    val dense_rankedDF = getRankedDataFrame(parsedDF,TopN_number,col_group_keys,spark=spark)
    dense_rankedDF.show(false)
    assert(dense_rankedDF.count === 14)
    // using rank
    val rankedDF = getRankedDataFrame(parsedDF,TopN_number,col_group_keys,false,spark=spark)
    rankedDF.show(100,false)
    assert(rankedDF.count === 10)
  }

  test("getRankedDataFrame should return proper ranking with only log_date column and log_uri ") {
    // Last row of data is incomplete
    val sourceDF = sourceToDataFrame(sourceFile, spark)
    val parsedDF = parseLogsDataFrame(sourceDF, spark)
    // Simple DataFrame Count Check
    assert(sourceDF.count === 500)
    assert(parsedDF.count === 500)

    //common config
    val col_group_keys = Seq("log_date","ip_address")
    val TopN_number = 2
    // Simple DataFrame Count Check
    // using dense_rank
    val dense_rankedDF = getRankedDataFrame(parsedDF,TopN_number,col_group_keys,spark=spark)
    dense_rankedDF.show(false)
    assert(dense_rankedDF.count === 4)

    //comparing full DataFrame
    val expectedDf = Seq(
      rankedBuilder(log_date = Date.valueOf("1995-07-01"), ip_address = "link097.txdirect.net", count = 13, rank = Some(1)),
      rankedBuilder(log_date = Date.valueOf("1995-07-01"), ip_address = "asp97-0.amsterdam.nl.net", count = 12, rank = Some(2)),
      rankedBuilder(log_date = Date.valueOf("1995-07-06"), ip_address = "jsc-b32-mac146.jsc.nasa.gov", count = 20, rank = Some(1)),
      rankedBuilder(log_date = Date.valueOf("1995-07-06"), ip_address = "128.159.146.92", count = 19, rank = Some(2)),
    ).toDF
    assertDataFrameEquals(dense_rankedDF, expectedDf)

    // using rank
    val rankedDF = getRankedDataFrame(parsedDF,TopN_number,col_group_keys,false,spark=spark)
    rankedDF.show(100,false)
    assert(rankedDF.count === 4)

  }


  // Helper function to get the schema of the Parsed DataFrame
  private def getParsedSchema(): StructType = {
    StructType(
      List(
        StructField("value", StringType),
        StructField("ip_address", StringType),
        StructField("log_date", DateType),
        StructField("client_request", StringType),
        StructField("methodType", StringType),
        StructField("osi", StringType),
        StructField("log_uri", StringType),
        StructField("log_response", StringType),
        StructField("log_size", LongType)
      )
    )
  }

}
