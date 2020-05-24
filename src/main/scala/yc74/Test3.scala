package yc74

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求：
 *   1. 所有电影中平均得分最高的前10部电影:    ( 分数,电影ID)
 * 2. 观看人数最多的前10部电影:   (观影人数,电影ID)
 */
/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Test3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO) //配置日志

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

    //所有电影中平均得分最高的前10部电影:    ( 分数,电影ID)
    val ratings = ratingsRDD.map(_.split("::")) //   [UserID,MovieID,Rating,Timestamp]
      .map(x => (x(0), x(1), x(2))) //  (UserID,MovieID,Rating )
    ratings.cache()
    println("平均得分最高的10部电影")

    println("简版的输出")
    ratings.map(x => (x._2, (x._3.toDouble, 1))) //     ( MovieId,( Rating, 1) )   (1252,(4.0,1))
      .reduceByKey((x, y) => {
        (x._1 + y._1, x._2 + y._2)
      }) //     ( MovieId,( 总分, 总次数) )
      .map(x => (x._2._1 / x._2._2, x._1)) //    (  平均分, MovieId )
      .sortByKey(false)
      .take(10)
      .foreach(println)

    println("按平均分取前10部电影输出详情:   (movieid,title, genre, 总评分,观影次数,平均分)   ")
    val moviesInfo = moviesRDD.map(_.split("::")) //    [MovieID,Title,Genres]
      .map(movie => {
        (movie(0), (movie(1), movie(2)))
      }) // ( MovieID,(Title,Genres))

    val ratingsInfo = ratings.map(x => (x._2, (x._3.toDouble, 1))) //     ( MovieId,( Rating, 1) )   (1252,(4.0,1))
      .reduceByKey((x, y) => {
        (x._1 + y._1, x._2 + y._2)
      }) //     ( MovieId,( 总分, 总次数) )
      .map(x => (x._1, (x._2._1 / x._2._2, x._2._1, x._2._2))) //    ( MovieId, (平均分, 总分,总次数) )

    moviesInfo.join(ratingsInfo) //   ( MovieID,  (   (Title,Genres),(平均分, 总分,总次数)  ) )
      .map(info => {
        (info._2._2._1, (info._1, info._2._1._1, info._2._1._2, info._2._2._2, info._2._2._3))
      }) // ( 平均分, (MovieID, itle,Genres,总分,总次数  ) )
      .sortByKey((false))
      .take(10)
      .foreach(println)


    println("观影人数最多的前10部电影")
    println("简版的输出(  观影人数，电影编号)")   //   ratings:  (UserID,MovieID,Rating )
    ratings.map(x => (x._2,  1)) //     ( MovieId, 1)
      .reduceByKey( (x, y) => {
        (x+y)
      }) //     ( MovieId,总次数 )
      .map( x => (x._2,x._1)) //    (  总次数, MovieId )
      .sortByKey(false)
      .take(10)
      .foreach(println)     //  286-> 999

    println("详情的输出(  观影人数，电影编号)")   //  ratingsInfo    ( MovieId, (平均分, 总分,总次数) )
    moviesInfo.join(ratingsInfo) //   ( MovieID,  (   (Title,Genres),(平均分, 总分,总次数)  ) )
      .map(info => {
        (info._2._2._3, (info._1, info._2._1._1, info._2._1._2, info._2._2._2, info._2._2._1))
      }) // ( 总次数, (MovieID, title,Genres,总分,平均分  ) )
      .sortByKey((false))
      .take(10)
      .foreach(println)   //  2858->  3428


    sc.stop()
  }
}
