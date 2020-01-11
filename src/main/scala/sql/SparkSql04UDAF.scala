package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}


/**
  * @author wb
  * @date 2019/11/25 20:42
  */
object SparkSql04UDAF {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

    val spark: SparkSession = SparkSession.builder()
      .appName("sparksql")
      .config(conf)
      .getOrCreate()

    //创建自定义函数
    val udaf = new myAgeAvgFunction
    //注册
    spark.udf.register("avgAge",udaf)

    val avgAge: DataFrame = spark.read.json("in/user.json")

    avgAge.createOrReplaceTempView("user")

    //spark.sql("select * from user").show
    spark.sql("select avgAge(age) from user").show

    //释放资源
    spark.stop()

  }
}
//继承 UserDefinedAggregateFunction
//实现方法
class myAgeAvgFunction extends UserDefinedAggregateFunction {

  //函数输入的数据结构
  override def inputSchema: StructType = {
     new StructType().add("age",LongType)
  }

  //计算时的数据结构，因为计算时会有缓冲
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  //函数返回的数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定
  override def deterministic: Boolean = true

  //计算之前的缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0L
  }

  //根据查询结果，更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getLong(0) +input.getLong(0)
    buffer(1)=buffer.getLong(1) +1

  }

  //将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getLong(0)+buffer2.getLong(0)
    //count
    buffer1(1) = buffer1.getLong(1)+buffer2.getLong(1)

  }

  //计算，最终的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}
