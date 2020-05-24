package yc74

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
需求8: 分析每年度不同类型的电影生产总数
*/

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Test8 {
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

    // 需求: 分析每年度不同类型的电影生产总数
    //MovieID::Title::Genres
    moviesRDD.map(x => x.split("::"))
      .map(x => (x(1), (1, x(2)))) //(   电影名,(1,类型) )  -> 从电影名中取出 year
      .map(item => { // (year, (1,类型))
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
          (-1, item._2)
        } else {
          (year.toInt, item._2)
        }
      }) //  (year, (1,类型) )
      .groupByKey() //   这不是reduceByKey ,而是groupByKey()   ->   RDD[(K, scala.Iterable[V])]     (年份，[(1,类型),(1,类型s)])
      .flatMapValues(array => { // [(1,类型s),(1,类型s)]      -> 结果应为:    (year,(1,类型) )    (year,(1,类型)
        var A: Map[String, Int] = Map() //    类型:次数
        array.foreach(item => { // (1,类型s)
          var count = item._1
          var types = item._2.split("\\|")
          for (t <- types) {
            if (A.contains(t)) {
              var oldCount = A.getOrElse(t, 0) + 1
              A += (t -> oldCount)
            } else {
              A += (t -> 1)
            }
          }
        })
        A
      }) // (  year,(类型，总数)
      .sortByKey()
      .collect()
      .foreach(println)

    sc.stop()
  }

}
