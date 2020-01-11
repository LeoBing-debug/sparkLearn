package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
*@author wb
*@date 2019/11/14 23:51
*/
object Spark05Oper {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("groupByFilter")

    val context: SparkContext = new SparkContext(conf)

    val listRdd: RDD[Int] = context.makeRDD(1 to 4)

    //生成数据按照指定规则分组
    //分组后的数据形成了对偶元组（K-V），K表示分组的key，V表示分组后的数据集合
    val groupByRdd: RDD[(Int, Iterable[Int])] = listRdd.groupBy(_%2)

    //过滤函数处理返回为true的元素组成
    val filterRdd: RDD[Int] = listRdd.filter(x=>x%2==0)

    groupByRdd.collect().foreach(println)
    println("=============")
    filterRdd.collect().foreach(println)

  }
}
