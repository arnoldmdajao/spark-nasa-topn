package com.secureworks.spark

import com.secureworks.spark.common.CommandLineArgs
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTopNProcessing  {
  /*
  usage:
  -f, --sourceFile <sourceFile>
                           Setting sourceFile is required
  -n, --TopN_number <value>
                           TopN_number is a top 'N' default is 10
  -c, --col_group_keys <value>
                           col_group_keys is an Seq of Columns to aggregate default:  Seq("log_date", "ip_address", "log_uri")
  -d, --useDenseRank <value>
                           useDenseRank is 'dense_rank' is use if set to true (Default) 'rank' if set to false
   */

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[CommandLineArgs]("spark-cassandra-example") {
      head("NASA Top N Daily", "1.0")
      opt[String]('f', "sourceFile").required().valueName("<sourceFile>").
        action((x, c) => c.copy(sourceFile = x)).
        text("Setting sourceFile is required")
      opt[Int]('n', "TopN_number").action( (x, c) =>
        c.copy(TopN_number = x) ).text("TopN_number is a top 'N' default is 10")
      opt[Seq[String]]('c', "col_group_keys").action( (x, c) =>
        c.copy(col_group_keys = x) ).text("col_group_keys is an Seq of Columns to aggregate default:  Seq(\"log_date\", \"ip_address\", \"log_uri\")")
      opt[Boolean]('d', "useDenseRank").action( (x, c) =>
        c.copy(useDenseRank = x) ).text("useDenseRank is 'dense_rank' is use if set to true (Default) 'rank' if set to false")
    }

    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .getOrCreate()

    parser.parse(args, CommandLineArgs()) match {
      case Some(config) =>
        val sourceDF = sourceToDataFrame(config.sourceFile, spark)
        val parsedDF = parseLogsDataFrame(sourceDF,spark)
        val dense_rankedDF = getRankedDataFrame(parsedDF, config.TopN_number, config.col_group_keys, config.useDenseRank, spark)

        //save to single csv file
        dense_rankedDF
          .repartition(1)
          .write
          .option("header", true)
          .mode("Overwrite")
          .csv("/tmp/result.csv")
        dense_rankedDF.show(1000,false)

      case None =>
      // arguments are bad, error message will have been displayed
    }


  }

  // Function to read from Source File to DataFrame
  def sourceToDataFrame(source: String,spark: SparkSession): DataFrame = {
    import spark.implicits._
    val resourceFile = getResourceFile(source)
    spark.sparkContext
      .textFile(resourceFile)
      .toDF()
  }

  /*
  Parse log line entry.  There are basically 2 patterns of log file entry to this data set
  This pattern is has protocol on the client request portion:
    199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
  The other pattern does not contain protocol
    pipe6.nyc.pipeline.com - - [01/Jul/1995:00:22:43 -0400] "GET /shuttle/missions/sts-71/movies/sts-71-mir-dock.mpg" 200 946425
  Its hard to spot by just looking at the data.  Its good to explore the entire source data to see the different data structures.
  There are many other technique in parsing log data but this is simple, more verbose and easier to understand.
  regexp_extract = extracting group of captured values with the parenthesis "()"
  group-1:ip,group-2:date,group-3:hour,group-4:min,group-5:sec,group-6:client_request,group-9:response,group-10:size
    client_request is further divided to group-1:uri,group-2:
      protocol is not used
      also time (hour,min,sec) is not used
  */

  def parseLogsDataFrame(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val logRegex = """^(\S+) - - \[(\S+):(\S+):(\S+):(\S+) -\S+] "(.*)" (\S+) (\S+)""" // Regex pattern to parse the log
    val clientReqRegex = """^(\S+) (\S+)""" // Regex pattern to parse the log

    df
      .withColumn("ip_address", regexp_extract($"value", logRegex, 1))
      .withColumn("log_date", to_date(regexp_extract($"value", logRegex, 2), "dd/MMM/yyyy"))
      .withColumn("client_request", regexp_extract($"value", logRegex, 6))
      .withColumn("methodType", regexp_extract($"client_request", clientReqRegex, 1))
      .withColumn("log_uri", regexp_extract($"client_request", clientReqRegex, 2))
      .withColumn("log_response", regexp_extract($"value", logRegex, 7).cast("Integer"))
      .withColumn("log_size", regexp_extract($"value", logRegex, 8).cast("Long"))
  }

  //function to get log entries that are not parsed by the pattern
  def getDiscardedDataFrame(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    df
      .filter((length($"log_date") === 0) || ($"log_date" isNull))
      .select($"value")
  }

  /*
  get the ranked DataFrame
  Assumption from the coding problem that its looking for Top N most frequent visitors (ip_address) and url (log_uri)
    i.e N = 3 Top3 daily count of ip_address and log_uri
  The code is flexible that you can Seq of column names to change the groupKeys filters
    val column_group_keys = Seq("log_date", "ip_address", "log_uri")
  By default 'dense_rank' is also passed in the spark-submit or can be fetch from the configuration if set to false 'rank' is used
  */
  def getRankedDataFrame(
                          df: DataFrame,
                          rankN: Int = 10,
                          col_group_keys: Seq[String] = Seq("log_date", "ip_address", "log_uri"),
                          useDenseRank: Boolean = true,
                          spark: SparkSession
                        ): DataFrame = {
    import spark.implicits._
    val groupKeys = col_group_keys.map(col(_))
    df
      .filter(length($"log_date") =!= 0)
      .groupBy(groupKeys: _*)
      .count
      .withColumn("rank",
        when(lit(useDenseRank), dense_rank.over(Window.partitionBy($"log_date").orderBy($"count".desc))
        ).otherwise(rank.over(Window.partitionBy($"log_date").orderBy($"count".desc)))
      )
      .filter($"rank" <= rankN)
      .orderBy($"log_date", $"rank")
  }


  // Helper function for getting data from Resources folder
  private def getResourceFile(fileName: String): String = {
    getClass.getResource(fileName).getPath
  }


}
