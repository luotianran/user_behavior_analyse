package com.bdp.tests

import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/13 下午2:33  
  *  @project bdp-user-analysis
  *  */
class SparkDemo extends FunSuite with BeforeAndAfter {
  self =>

  var spark: SparkSession = _

  before {
    spark = SparkSession.builder().appName("tests").master("local[*]").getOrCreate()
  }

  object sparkSqlmplicits extends SQLImplicits {
    override protected def _sqlContext: SQLContext = self.spark.sqlContext
  }

  import sparkSqlmplicits._

  test("test")  {

    val rdd = spark.sparkContext.parallelize(1 to 10)
    rdd.map(_ + 1).foreach(println(_))
  }

}
