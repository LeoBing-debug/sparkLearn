package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
*@author wb
*@date 2019/11/17 17:33
*/
object Spark06Oper {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")

    val context: SparkContext = new SparkContext(conf)

    val listRdd: RDD[Int] = context.makeRDD(1 to 10)

    //withReplacement:抽样数据是否放回
    //fraction:抽样出的随机数
    //seed:随机数生成的种子
    val sampleRdd: RDD[Int] = listRdd.sample(false,0.4,1)

    sampleRdd.collect().foreach(println)

  }
}
