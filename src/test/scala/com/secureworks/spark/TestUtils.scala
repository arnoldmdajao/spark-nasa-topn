package com.secureworks.spark

import org.apache.spark.sql._




trait SharedSparkSessionBase {
  /**
   */
   def spark: SparkSession = SparkSession.builder.master("local[*]").getOrCreate()

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `spark.implicits._` is not possible here.
   * This is because we create the `SQLContext` immediately before the first test is run,
   * but the implicits import is needed in the constructor.
   */
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }
}
