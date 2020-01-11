package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
*@author wb
*@date 2019/11/13 23:45
*/
object Spark03Oper {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("spark03")

    val context: SparkContext = new SparkContext(conf)

    val listRdd: RDD[List[Int]] = context.makeRDD(Array(List(1,2),List(3,4)))

    val flatMapRdd: RDD[Int] = listRdd.flatMap(datas=>datas)//很生动，flatMap 就是展开

    val maprdd: RDD[(Int, Int)] = flatMapRdd.map((_,1))

    flatMapRdd.collect().foreach(println)
    maprdd.collect().foreach(println)
  }
}
