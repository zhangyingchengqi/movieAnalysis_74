package oldVersion_before74

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SparkBatch {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("hello world").setMaster("local[6]");
    val sc = new JavaSparkContext(conf);

    var arr = ListBuffer[String]()
    for (i <- 0 to 100) {
      arr.append("张" + i)
    }
    println("总共有待处理数据:" + arr.length)

    val rdd = sc.parallelize(arr)



    rdd.mapPartitions(elements => {
      val con = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "a")
      con.setAutoCommit(false);
      val pstmt = con.prepareStatement("insert into person( name) values(?)")
      var result = new ArrayBuffer[String]()
      //println("每批次处理数据条数:" + elements.length)

      for (item <- elements) {
        println(item)
        pstmt.setString(1, item)
        pstmt.addBatch()  //添加到批中
        result.+=(item)
      }
      pstmt.executeBatch();
      con.commit()
      con.setAutoCommit(true)
      con.close()
      println("添加一批数据成功")

      result.iterator
    }).foreach(println)

  }
}
