package com.secureworks.spark.builders

case class NasaLogBuilder(
                           ip_address:String = "199.72.81.55",
                           log_date:String = "01/Jul/1995",
                           client_request:String = "GET /history/apollo/ HTTP/1.0",
                           methodType:String = "GET",
                           log_uri:String = "/history/apollo/",
                           log_response:String = "200",
                           log_size:String = "6245"
                         )
