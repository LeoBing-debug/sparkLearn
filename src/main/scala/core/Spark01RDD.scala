package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
*@author wb
*@date 2019/11/10 22:33
*/
object Spark01RDD {
  def main(args: Array[String]): Unit = {
    val myConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("my")

    val context: SparkContext = new SparkContext(myConf)

    //两种方式，底层实现都是parallelize
    //val value: RDD[Int] = context.parallelize(Array(1,2,3,4))
    val unit: RDD[Int] = context.makeRDD(List(1,2,3,4))
    context.textFile("")
    //unit.collect().foreach(print)

    unit.saveAsTextFile("o")

  }
}
