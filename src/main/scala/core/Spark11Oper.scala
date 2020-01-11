package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wb
  * @date 2019/11/17 18:04
  */
object Spark11Oper {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("distinct")

    val context: SparkContext = new SparkContext(conf)

    val listRdd: RDD[(String, Int)] = context.makeRDD(List(("a",3),("b",1),("b",5),("c",6),("c",1),("a",8)),2)

    //方法中有两个函数
    //1、初始值：scala中成对出现
    //2、第一个max代表分区内的处理方法（取最大值），第二个参数代表分区间的处理方法（相同key相加）
    val aggregateByKeyRdd: RDD[(String, Int)] = listRdd.aggregateByKey(0)(math.max(_,_),_+_)

    val foldByKeyRdd: RDD[(String, Int)] = listRdd.foldByKey(0)(_+_)

    val combineByKeyRdd: RDD[(String, (Int, Int))] = listRdd.combineByKey((_,1),(acc:(Int,Int),v)=>(acc._1+v,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
    //aggregateByKeyRdd.collect().foreach(println)
    //foldByKeyRdd.collect().foreach(println)

    combineByKeyRdd.collect().foreach(println)

  }
}
