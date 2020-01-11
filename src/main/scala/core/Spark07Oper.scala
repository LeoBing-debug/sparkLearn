package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
*@author wb
*@date 2019/11/17 18:04
*/
object Spark07Oper {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("distinct")

    val context: SparkContext = new SparkContext(conf)

    val listRdd: RDD[Int] = context.makeRDD(List(1,2,4,5,2,1))

    //val distinctRdd: RDD[Int] = listRdd.distinct()
    //disctinct去重之后会导致数据减少，所以可以改变默认的分区数
    val distinctRdd: RDD[Int] = listRdd.distinct(2)

    distinctRdd.collect().foreach(println)

  }
}
