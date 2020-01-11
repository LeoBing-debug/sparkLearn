package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
  * @author wb
  * @date 2019/11/25 20:42
  */
object SparkSql03Transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

    val spark: SparkSession = SparkSession.builder()
      .appName("sparksql")
      .config(conf)
      .getOrCreate()

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",30)))

    //进行转换之前，需要引入转换规则，这里的spark不是包名的含义，是spark对象的名字
    import spark.implicits._

    val userRdd: RDD[user] = rdd.map {
      case (id, name, age) => {
        user(id, name, age)
      }
    }
    val userDs: Dataset[user] = userRdd.toDS()

    //类型就是User
    val rdd1: RDD[user] = userDs.rdd

    rdd1.foreach(println)
    //释放资源
    spark.stop()

  }
}
