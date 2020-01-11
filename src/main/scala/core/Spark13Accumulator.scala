package core

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wb
  * @date 2019/11/17 18:04
  */
object Spark13Accumulator {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("distinct")

    val context: SparkContext = new SparkContext(conf)

    val listRdd: RDD[String] = context.makeRDD(List("hadoop","hive","spark" ),2)

    val sum: Int = 0

    //初始化累加器
    val longaccumulator: LongAccumulator = context.longAccumulator

    //TODO 创建累加器
    val accumulator = new WordAccumulator
    //注册累加器
    context.register(accumulator)


    listRdd.foreach{
      case word=>{
        //使用累加器
        accumulator.add(word)
      }
    }
    //读取累加器的值
    print(accumulator.value)
  }

}

//声明累加器
//1、继承AccumulatorV2
//2、实现抽象方法
//3、创建累加器
class WordAccumulator extends AccumulatorV2[String,util.ArrayList[String]]{

  val list = new util.ArrayList[String]()

  //当前累加器是否为初始化
  override def isZero: Boolean = list.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] ={
    new WordAccumulator
  }

  //重置累加器
  override def reset(): Unit = list.clear()

  //向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("h")){
      list.add(v)
    }
  }

  //合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //获取累加器的结果
  override def value: util.ArrayList[String] = list
}
