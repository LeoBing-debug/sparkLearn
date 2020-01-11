package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
*@author wb
*@date 2019/11/14 23:29
*/

object Spark04Oper {
  def main(args: Array[String]): Unit = {
    val glomConf: SparkConf = new SparkConf().setMaster("local").setAppName("glom")

    val context: SparkContext = new SparkContext(glomConf)

    val listRdd: RDD[Int] = context.makeRDD(List(1,2,3,4,5),2)

    val glomRdd: RDD[Array[Int]] = listRdd.glom()

    //将一个分区中的数放到一个数组中
    glomRdd.collect().foreach(array=>{
      println(array.mkString(","))
    })

  }
}
