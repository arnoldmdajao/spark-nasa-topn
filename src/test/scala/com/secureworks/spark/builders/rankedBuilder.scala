package com.secureworks.spark.builders

import java.sql.Date

case class rankedBuilder (
                           log_date:Date = "1995-07-06".asInstanceOf[Date],
                           ip_address:String = "",
                           count:Long = 0L,
                           rank:Option[Int] = Some(1),
                         )
