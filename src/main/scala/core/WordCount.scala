package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wb
  * @date 2019/11/9 23:19
  */
//：符号，分割变量和类型
object WordCount {

  def main(args: Array[String]): Unit = {

    //local模式
    //创建sparkconf对象
    //设定Spark计算框架的运行部署环境
    val conf : SparkConf = new SparkConf().setMaster("local").setAppName("app")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //读取文件，将文件内容一行一行读取出来
    val unit :RDD[String] = sc.textFile("word/word.txt")

    //将数据一行一行分解开来
    val words :RDD[String] = unit.flatMap(_.split(" "))

    //为了统计方便，可以使用spark的算子转换
    val wordsToOne :RDD[(String,Int)] = words.map((_,1))

    //对数据进行分组聚合
    val wordTosum :RDD[(String,Int)] = wordsToOne.reduceByKey(_+_)

    //将统计结果打印到控制台
    val result :Array[(String,Int)] = wordTosum.collect();

    result.foreach(println)

  }
}
