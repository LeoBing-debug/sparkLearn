package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wb
  * @date 2019/11/17 18:04
  */
object Spark08Oper {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("distinct")

    val context: SparkContext = new SparkContext(conf)

    val listRdd: RDD[Int] = context.makeRDD(1 to 16 ,4)
    println(listRdd.partitions.size)

    //缩减分区，其实是合并分区，类似region操作
    val coalesceRdd: RDD[Int] = listRdd.coalesce(2)
    println(coalesceRdd.partitions.size)

    //重新分区，底层就是调用coalesce
    val repartitionRdd: RDD[Int] = listRdd.repartition(2)
    repartitionRdd.collect().foreach(println)

    //sortBy
    val sortByRdd: RDD[Int] = listRdd.sortBy(x=>x,false)
    sortByRdd.collect().foreach(println)

    //coalesceRdd.saveAsTextFile("out")

  }
}
