FROM bde2020/spark-submit:2.4.5-hadoop2.7

MAINTAINER Arnold Dajao <arnoldmd@gmail.com>

ENV SPARK_APPLICATION_MAIN_CLASS com.secureworks.spark.SparkTopNProcessing
ENV SPARK_APPLICATION_ARGS "-f /data/NASA_access_log_Jul95.gz"

COPY . /usr/src/app
RUN cd /usr/src/app && sbt clean assembly