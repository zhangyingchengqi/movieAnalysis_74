package oldVersion_before74

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Movie_RDD_3 {
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

    //需求3: 所有电影中平均得分最高的前10部电影及观看人数最多的前10部电影
    // 所有电影中平均得分最高的前10部电影
    val ratings = ratingsRDD.map(_.split("::")) // UserID,MovieID,Rating,Timestamp
      .map(x => (x(0), x(1), x(2))) // ( UserID, MovieId,Rating )
    ratings.cache()
    println("平均得分最高的前10部电影")

    println("简版输出: ( 分数,电影ID):")
    ratings.map(x => (x._2, (x._3.toDouble, 1))) //    (MovieID,(Rating,1 ) )
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // ( MovieID, ( 总评分，总次数) )
      .map(x => (x._2._1.toDouble / x._2._2, x._1)) // ( 平均分, MovieID )
      .sortByKey(false) // RDD:(Double,String)
      .take(10) //  Array[(Double, String)]
      .foreach(println)

    println("详情输出: ( 分数,电影ID):")
    val moviesInfo = moviesRDD.map(_.split("::"))
      .map(movie => {
        (movie(0), (movie(1), movie(2)))
      }) //    (MovieId, (Title,Genres))   //电影详情
    val topRatingRDD = ratings.map(x => (x._2, (x._3.toDouble, 1))) //    (MovieID,(Rating,1 ) )
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // ( MovieID, ( 总评分，总次数) )
      .map(x => (x._2._1.toDouble / x._2._2, x._1)) // ( 平均分, MovieID )
      .map(x => (x._2, x._1))

    val topRatingMovie = topRatingRDD.join(moviesInfo) // (MovieId,(平均分,(Title,Genres)))
      .map(x => (x._2._1, (x._1, x._2._2._1, x._2._2._2))) // ( 平均分,(MovieId,title,genre))
      .sortByKey(false)
      .take(10)
    topRatingMovie.foreach(println)

    // 观看人数最多的前10部电影
    println("观看人数最多的前10部电影")
    println("简版输出: ( 观影人数,电影ID):")
    ratings.map(x => (x._2, 1)) //    (MovieID,1 )
      .reduceByKey((x, y) => (x + y)) // ( MovieID, 总次数)
      .map(x => (x._2, x._1)) // (总次数,MovieID)
      .sortByKey(false) // RDD:(Int,String)
      .take(10) //  Array[(Double, String)]
      .foreach(println)

    println("详情输出: ( 分数,电影ID):")
    val movieWatchedRDD = ratings.map(x => (x._2, 1)) //    (MovieID,1 )
      .reduceByKey((x, y) => (x + y)) // ( MovieID, 总次数)
    val result = movieWatchedRDD.join(moviesInfo) //   ( MovieId,(总次数,(Title,Genres) ))
      .map(x => (x._2._1, (x._1, x._2._2._1, x._2._2._2)) )// ( 总次数,(MovieId,Title,Genres ))
        .sortByKey(false)
        .take(10)
        result.foreach(println)


    sc.stop()
  }
}
