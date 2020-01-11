package stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
*@author wb
*@date 2019/11/30 14:43
*/
object wordcount {
  def main(args: Array[String]): Unit = {
    val wordcount: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    val context = new StreamingContext(wordcount,Seconds(3))

    //DStream
    val socketDstream: ReceiverInputDStream[String] = context.socketTextStream("localhost",9999)

    val wordDstream: DStream[String] = socketDstream.flatMap(line=>line.split(" "))

    val mapDstream: DStream[(String, Int)] = wordDstream.map((_,1))

    val reduceByKeyDstream: DStream[(String, Int)] = mapDstream.reduceByKey(_+_)

    reduceByKeyDstream.print()

    //特殊的两行
    context.start()
    context.awaitTermination()



  }
}
