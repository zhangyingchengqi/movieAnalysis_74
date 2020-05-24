package yc74

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求：
 * 1. 某个用户看过的电影数量
 * 2. 这些电影的信息，格式为:  (MovieId,Title,Genres)
 */
/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO) //配置日志

    var userId="18"

    var masterUrl = "local[2]"
    var appName = "movie analysis"
    //  将来上线后 jar， submit  提交到服务器，可以命令行传参
    if (args.length > 0) {
      masterUrl = args(0)
      userId=args(1)
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

    //某个用户看过的电影数量    UserID::MovieID::Rating::Timestamp
    val userWatchedMovie=ratingsRDD.map(    _.split("::")  )    // [  UserID,MovieID,Rating,Timestamp ]
      .map(   item=>{ ( item(1), item(0)  ) } )    //    将数组中提取两个元素 转为  元组   (   MovieId, UserId)
      .filter(   _._2.equals(   userId )   )

//    val userWatchedMovie=ratingsRDD.map(    _.split("::")  )    // [  UserID,MovieID,Rating,Timestamp ]
//      .filter(    item=>  item(0).equals(   userId )   )        //数组
    println(userId+"观看过的电影数: "+  userWatchedMovie.count() )


    println("这些电影的详情:\n")
    //需求二: 这些电影的信息，格式为:  (MovieId,Title,Genres)
    //   userWatchedMovie 与  moviesRDD   join
    //    userWatchedMovie  =>   (MovieID,UserId)    ->已完成
    //     moviesRDD     =>    (  MovieId, (Title,Genres) )
    val movieInfoRDD=moviesRDD.map(   _.split("::"))     //      [MovieID,Title,Genres]
        .map(    movie=> {  (movie(0),  (movie(1),movie(2))   ) } )     //  (  MovieId, (Title,Genres) )

    val result=userWatchedMovie.join(  movieInfoRDD )       // (  MovieId,(UserId,(Title,Genres) )      )
        .map(    item=>{   (item._1,   item._2._1,  item._2._2._1,   item._2._2._2  ) } )

    result.foreach( println )

    sc.stop()

  }
}
