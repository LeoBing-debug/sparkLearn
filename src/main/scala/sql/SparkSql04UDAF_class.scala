package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}

/** UDAF和 UDAF_CLASS 的区别就类似于 DataFrame 和 Datasets 的区别
  * 有了类型之后，就有了面向对象的含义，操作起来就比较方便
  * @author wb
  * @date 2019/11/25 20:42
  */
object SparkSql04UDAF_class {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

    val spark: SparkSession = SparkSession.builder()
      .appName("sparksql")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val udaf = new myAgeAvgClassFunction

    //直接把函数转成列
    val avg: TypedColumn[UserBean, Double] = udaf.toColumn.name("avg") //查询显示的列的名字

    val frame: DataFrame = spark.read.json("in/user.json")

    val userBean: Dataset[UserBean] = frame.as[UserBean]

    //dsl
    userBean.select(avg).show()
    //释放资源
    spark.stop()

  }
}

//样例类
case class UserBean(name:String,age: BigInt)
case class AvgBuffer(var sum:BigInt,var count:Int)

//继承：Aggregator
//

class myAgeAvgClassFunction extends Aggregator [ UserBean , AvgBuffer , Double ]{

  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0,0)
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum=b.sum+a.age
    b.count=b.count+1

    b
  }
  //缓冲区操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.count=b1.count+b2.count
    b1.sum=b1.sum+b2.sum

    b1
  }

  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble/reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}