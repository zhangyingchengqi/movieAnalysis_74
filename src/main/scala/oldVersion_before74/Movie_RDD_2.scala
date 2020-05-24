package oldVersion_before74

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 *  数据：
 *  1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 *  3，"movies.dat"：MovieID::Title::Genres
 *  4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Movie_RDD_2 {
  def main(args: Array[String]) {
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

    occupationsRDD.cache()
    usersRDD.cache()
    ratingsRDD.cache()
    moviesRDD.cache()

    val userId="18"
    //需求2:  求出 特定用户(  18  )看过的电影 的总数，并打印这些电影的详情
    val specialUserMovie = ratingsRDD.map(_.split("::")) // UserID,MovieID,Rating,Timestamp
      .map(rating => { (rating(1), rating(0)) }) // (MovieId,UserID)
      .filter(_._2.equals(userId))
    println(  userId+"号用户看过的电影数量:"+ specialUserMovie.count() )

    // MovieID::Title::Genres
    val moviesInfo = moviesRDD.map(_.split("::"))
      .map(movie => { (movie(0), (movie(1), movie(2))) })   //    (MovieId, (Title,Genres))
    val result = specialUserMovie.join(moviesInfo)   //  (MovieId,(UserId,(Title,Genres)))
    //简化输出结果为:    (MovieId,Title,Genres)
    val result2=result.map(   item => { ( item._1,item._2._2._1,item._2._2._2 )   } )
    println(  userId+"号用户看过的电影有:" )
    result2.foreach(  println  )

    sc.stop()
  }
}
