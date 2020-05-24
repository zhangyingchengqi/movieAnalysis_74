package yc74

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
需求7: 分析每年度生产的电影总数
输出格式:   (年度,数量)
*/

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Test7 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志

    var masterUrl = "local[2]"
    var appName = "movie analysis"
    //  将来上线后 jar， submit  提交到服务器，可以命令行传参
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      appName = args(1)
    }
    //创建上下文
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl);
    val sc = new SparkContext(conf);
    val filepath = "data/"
    val usersRDD = sc.textFile(filepath + "users.dat")
    val occupationsRDD = sc.textFile(filepath + "occupations.dat")
    val ratingsRDD = sc.textFile(filepath + "ratings.dat")
    val moviesRDD = sc.textFile(filepath + "movies.dat")

    moviesRDD.cache()

    /*
    输出格式:   (年度,数量)
     */
    moviesRDD.map(x => x.split("::"))
      .map(x => (x(1), 1)) //(   电影名,1)  -> 从电影名中取出 year
      .map(item => { // (year, 1)
        var mname = ""
        var year = ""
        val pattern = Pattern.compile(" (.*) (\\(\\d{4}\\))") // Toy Story (1995)      (.*) (\\(\\d{4}\\))
        val matcher = pattern.matcher(item._1)
        if (matcher.find()) {
          mname = matcher.group(1)
          year = matcher.group(2)
          year = year.substring(1, year.length() - 1)
        }
        if (year == "") {
          (-1, 1)
        } else {
          (year.toInt, 1)
        }
      })
      .reduceByKey((x, y) => x + y)
      .sortByKey()     //  RDD   :  分区      2个区   每个区中的数据有序    A-> 1输出    B-> 1输出,  只能保证分区中的数据有序，而不同分区的数据直接输出的话无序.
      .collect()    //将数据取出来，传到driver端，此时是对有序数组进行排序，所以不管是几个分区，最后输出的数据已经排好序了.
      .foreach(println)


    sc.stop()
  }

}
