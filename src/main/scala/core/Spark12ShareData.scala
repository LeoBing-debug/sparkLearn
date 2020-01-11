package core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wb
  * @date 2019/11/17 18:04
  */
object Spark12ShareData {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("distinct")

    val context: SparkContext = new SparkContext(conf)

    val listRdd: RDD[Int] = context.makeRDD(List(1,2,3,4),2)

    val sum: Int = 0

    //初始化累加器
    val accumulator: LongAccumulator = context.longAccumulator

    listRdd.foreach{
      case i=>{
        //使用累加器
        accumulator.add(i)
      }
    }
    //读取累加器的值
    print(accumulator.value)

  }
}
