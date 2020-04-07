package com.secureworks.spark.common

case class CommandLineArgs(sourceFile: String = "", // required
                           TopN_number: Int = 5,
                           col_group_keys: Seq[String] = Seq("log_date", "ip_address", "log_uri"), // default is Seq("log_date", "ip_address", "log_uri")
                           useDenseRank: Boolean = true
                          )
