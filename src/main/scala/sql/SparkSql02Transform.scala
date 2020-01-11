package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
  * @author wb
  * @date 2019/11/25 20:42
  */
object SparkSql02Transform {
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

    //转换为DF
    val df: DataFrame = rdd.toDF("id","name","age")

    //转换为DS
    val ds: Dataset[user] = df.as[user]

    //转换回DF
    val df1: DataFrame = ds.toDF()

    //转换回RDD
    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(row=>{
      //获取数据可以通过索引获取数据
      //print(row.getInt(1))
      println(row.getString(1))
    })

    spark.stop()

  }
}

case class user(id:Int,name:String,age:Int)
