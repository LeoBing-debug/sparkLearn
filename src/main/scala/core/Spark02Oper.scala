package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
*@author wb
*@date 2019/11/12 22:32
*/
object Spark02Oper {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("spark02")

    val context: SparkContext = new SparkContext(conf)


    val listRdd: RDD[Int] = context.makeRDD((1 to 8),3)

    val i = 10
    //map算子
    //对每条数据进行处理
    val mapRdd: RDD[Int] = listRdd.map(x=>x*i) //(_*2)

    //mapPartitions 可以对一个RDD中的所有分区进行遍历
    //mapPartitions 效率优于 map 算子，减少发送到执行器的交互次数
    //mapPartitions 可能会出现内存溢出（OOM），当一个分区对象较大时
    val mapPartitionsRdd: RDD[Int] = listRdd.mapPartitions(datas => {
      datas.map(data => data * 2)//这行代码属于整个计算，map 不是算子，是scala的map，只有mapPartitions是算子
    })

    val index: RDD[(Int,String)] = listRdd.mapPartitionsWithIndex {
      //传过来的数据不是一条，就要{}括号处理，从这里可以看出数据的处理逻辑是自定义的，但是要遵守算子的规则
      case (num, datas) => {
        datas.map((_, "分区号:" + num))//元组
      }
    }

    //mapRdd.collect().foreach(println)
    //mapPartitionsRdd.collect().foreach(println)
    index.collect().foreach(println)

  }

}
