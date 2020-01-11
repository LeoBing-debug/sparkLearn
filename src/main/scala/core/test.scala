package core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wb
  * @date 2019/10/30 22:27
  */
object test {
    def main(args: Array[String]):Unit={
      calculator()
    }

    def calculator(){
      val conf = new SparkConf().setAppName("wbSpark")
      conf.setMaster("local")

      val sc = new SparkContext(conf)
      val rdd = sc.parallelize(List(1,2,3,4,5,6)).map(_*3)

      print(rdd)

      val mappedRDD = rdd.filter(_>10).collect()
      //对集合求和
      println(rdd.reduce(_+_))
      //输出大于10的元素
      for(arg <- mappedRDD)
        print(arg+"")
      println()
      println("work")

    }

}
