package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wb
  * @date 2019/11/17 18:04
  */
object Spark10Oper {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("distinct")

    val context: SparkContext = new SparkContext(conf)

    val listRdd: RDD[(Int, String)] = context.makeRDD(List((1,"a"),(2,"b"),(3,"c"),(2,"d")),4)

    //默认对Key进行操作，生成一个sequence
    val groupByKey: RDD[(Int, Iterable[String])] = listRdd.groupByKey()

    //将相同key值聚合到一起，第二个参数可以设置reduce个数（也就是任务数/分区数）
    val reduceByKeyRdd: RDD[(Int, String)] = listRdd.reduceByKey((x,y)=>{x+"===="+y},2)

    //groupByKey.collect().foreach(println)
    reduceByKeyRdd.collect().foreach(println)
  }
}
