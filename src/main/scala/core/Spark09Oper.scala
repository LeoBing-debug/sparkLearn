package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @author wb
  * @date 2019/11/17 18:04
  */
object Spark09Oper {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("distinct")

    val context: SparkContext = new SparkContext(conf)

    val listRdd: RDD[(Int, String)] = context.makeRDD(List((1,"a"),(2,"b"),(3,"c")),4)

    val partitionByRdd: RDD[(Int, String)] = listRdd.partitionBy(new myPartition(2))

    //listRdd.saveAsTextFile("out")
    partitionByRdd.saveAsTextFile("out")

  }

  //声明分区器
  //自定义处理方式
  class myPartition(partitions: Int) extends Partitioner{
    override def numPartitions: Int = {
      partitions
    }

    //自己处理
    override def getPartition(key: Any): Int = {
      1
    }
  }

}
