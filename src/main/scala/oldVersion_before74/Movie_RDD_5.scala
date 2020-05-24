package oldVersion_before74

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 *
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 *  数据：
 *  1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 *  3，"movies.dat"：MovieID::Title::Genres
 *  4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Movie_RDD_5 {
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

    val usersRDD = sc.textFile(filepath + "users.dat")
    val occupationsRDD = sc.textFile(filepath + "occupations.dat")
    val ratingsRDD = sc.textFile(filepath + "ratings.dat")
    val moviesRDD = sc.textFile(filepath + "movies.dat")

    occupationsRDD.cache()
    usersRDD.cache()
    ratingsRDD.cache()
    moviesRDD.cache()

    //需求5: 分析最受不同年龄段人员欢迎的电影的前10
    //原始数据集中的年龄段划分
    //   under 18: 1
    //    18-24:   18
    //  25-34:   25
    // 35-44: 35
    // 45-49:  45
    // 50-55:  50
    // 56+   56
    // TODO:    目标客户     :    16-35     王者:  16-30       花彭 60+
    var age="1"
    println("\n年龄段为:"+age+"喜爱的电影TOP 10: ")
    val userWithAge=usersRDD.map( x=> x.split("::"))     // UserID::Gender::Age::OccupationID::Zip-code
            .map( x=> ( x(0),x(2))  )   // UserID::Gender::Age::OccupationID::Zip-code
            .filter(  _._2.equals(  age )        )

    val ratings = ratingsRDD.map(_.split("::")) // UserID,MovieID,Rating,Timestamp
      .map(x => (x(0), x(1), x(2))) // ( UserID, MovieId,Rating )
    ratings.cache()

    //取出指定年龄的用户评过的电影的打分
    val ratingwithage = ratings.map(x => (x._1, (x._2, x._3))) // ( UserID, (MovieId,Rating) )
                                  .join(userWithAge)      //    UserID, age
                          // ratingwithage : (用户编号,((movieID,打分),年龄))
    ratingwithage.map( x => (x._2._1._1,(  x._2._1._2.toDouble, 1  ))   ) //  (   MovieId, ( Rating, 1)  )
      .reduceByKey(  (x, y) => (x._1 + y._1, x._2+y._2 )   )    //  (  MovieID,(总评分) )
      .map(item => (   item._2._1/item._2._2 ,   item._1        ))
      .sortByKey(false)
      .map(   x=> (x._2,x._1) )
      .take(10).foreach(println)


    //加入电影名称显示
    // MovieID::Title::Genres
    val rdd=ratingwithage.map( x => (x._2._1._1,(  x._2._1._2.toDouble, 1  ))   ) //  (   MovieId, ( Rating, 1)  )
      .reduceByKey(  (x, y) => (x._1 + y._1, x._2+y._2 )   )    //  (  MovieID,(总评分) )
      .map(item => (   item._2._1/item._2._2 ,   item._1        ))
      .map(   x=> (x._2,x._1) )


    val movieInfos = moviesRDD.map(x => x.split("::"))
      .map(x => (x(0), x(1)))

    rdd.join(movieInfos) //  (  movieId,(平均分,电影名) )
      .map(x => (x._2._1, (x._1, x._2._2))) // (  平均分,(movieId,电影名))
      .sortByKey(false) //按平均分排序
      .map(x => (x._2._1, x._2._2.substring(0, x._2._2.indexOf(" (")),  x._1)) //  ( movieId, 电影名,平均分)
      .take(10)
      .foreach(println)


    sc.stop()
  }
}
