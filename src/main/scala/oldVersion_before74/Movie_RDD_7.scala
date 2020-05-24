package oldVersion_before74

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 *  数据：
 *  1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 *  3，"movies.dat"：MovieID::Title::Genres
 *  4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Movie_RDD_7 {
  def main(args: Array[String]) {
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
    val moviesRDD = sc.textFile(filepath + "movies.dat")
    val usersRDD = sc.textFile(filepath + "users.dat")

    moviesRDD.cache()


    //需求7: 分析每年度生产的电影总数     MovieID::Title::Genres
    moviesRDD.map(x => x.split("::"))
      .map(x => (x(1), 1))
      .map(item => {
        var year = ""
        var mn = ""
        //   Oliver! (1968)
        val pattern = Pattern.compile("(.*) (\\(\\d{4}\\))");
        val matcher = pattern.matcher(item._1);
        if (matcher.find()) {
          mn = matcher.group(1)
          year = matcher.group(2)
          year = year.substring(1, year.length() - 1)
        }
        if(   year==""){
          ( -1,1)
        }else {
          (year.toInt, 1)
        }
      })
      .reduceByKey((x, y) => x + y)
      .sortByKey()
     .foreach(println)






    //while (true) {}
    sc.stop()
  }
}
