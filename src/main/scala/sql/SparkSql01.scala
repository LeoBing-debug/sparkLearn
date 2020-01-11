package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author wb
  * @date 2019/11/25 20:42
  */
object SparkSql01 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

    val spark: SparkSession = SparkSession.builder()
      .appName("sparksql")
      .config(conf)
      .getOrCreate()

    val frame: DataFrame = spark.read.json("in/user.json")

    frame.createOrReplaceTempView("user")

    spark.sql("select * from user").show()
    //frame.show()

    spark.stop()

  }
}
