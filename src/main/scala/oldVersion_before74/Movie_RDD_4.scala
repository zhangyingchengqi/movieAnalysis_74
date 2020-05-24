package oldVersion_before74

/**
 *
 * 需求4:
 * 分析男性用户最喜欢看的前10部电影
 * 女性用户最喜欢看的前10部电影
 *
 * 这里我先计算了按性别累计总分最高的前10部电影
 * 接着，再按性别计算每部电影的   总评分，观影次数，平均分     ，这样就可以更好的按不同的列的需求来排序
 *
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Movie_RDD_4 {
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

    val ratings = ratingsRDD.map(_.split("::")) // UserID,MovieID,Rating,Timestamp
      .map(x => (x(0), x(1), x(2))) // ( UserID, MovieId,Rating )
    ratings.cache()

    //需求4: 分析男性用户最喜欢看的前10部电影及女性用户最喜欢看的前10部电影
    //  先合并  ratings和users  (只加入  gender )
    val ratingwithgender = ratings.map(x => (x._1, (x._2, x._3))) // ( UserID, (MovieId,Rating) )
      .join(
        usersRDD.map(_.split("::")) // // UserID,Gender,Age,OccupationID,Zip-code
          .map(x => (x(0), x(1))) //    UserID, Gender
      )
    //ratingwithgender.take(10).foreach(println)   // (userID,((MovieID, rating),gender))

    //  (userID,((MovieID, rating),gender))
    val femaleRatings = ratingwithgender.filter(item => item._2._2.equals("F")) //女性的评分
    // (userID,((MovieID, rating),gender))
    val maleRatings = ratingwithgender.filter(item => item._2._2.equals("M")) //男性的评分

    //输出女性和男性用户对电影评分的总分
    //女性评分最高的10部电影
    println("女性评分总分最高的10部电影, 格式:    总评分,电影ID")
    femaleRatings.map(x => (x._2._1._1, (x._2._1._2.toDouble))) //  (   MovieId, ( Rating)  )
      .reduceByKey((x, y) => x + y) //  (  MovieID,(总评分) )
      .map(item => (item._2, item._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)

    println("男性评分总分最高的10部电影, 格式:    总评分, 电影ID")
    maleRatings.map(x => (x._2._1._1, (x._2._1._2.toDouble))) //  (   MovieId, ( Rating)  )
      .reduceByKey((x, y) => x + y) //  (  MovieID,(总评分) )
      .map(item => (item._2, item._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)

    //  改进版 :   累计总分后计算平均分,再按平均分排序,显示Top 10 电影时，显示电影名
    //女性对电影的平均分
    // 输入RDD格式:  (userID,((MovieID, rating),gender))
    val femaleFavoriteMovie = femaleRatings.map(x => (x._2._1._1, (x._2._1._2.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // (movieid,(总分数,打分次数))
      .map(x => (x._1, (x._2._1, x._2._2, x._2._1 / x._2._2))) // ( movieid, (总分,打分次数, 平均分) )


    //男性对电影的平均分
    // 输入RDD格式:  (userID,((MovieID, rating),gender))
    val maleFavoriteMovie = maleRatings.map(x => (x._2._1._1, (x._2._1._2.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // (movieid,(总分数,打分次数))
      .map(x => (x._1, (x._2._1, x._2._2, x._2._1 / x._2._2)) )// ( movieid, (总分,打分次数, 平均分) )

    //加入电影名称显示
    // MovieID::Title::Genres
    val movieInfos = moviesRDD.map(x => x.split("::"))
      .map(x => (x(0), x(1)))

    println("1.按平均分计算最受女性喜欢的电影TOP10")
    femaleFavoriteMovie.join(movieInfos) //  (  movieId,((总分,打分次数, 平均分),电影名))
      .map(x => (x._2._1._3, (x._1, x._2._1._1, x._2._1._2, x._2._2))) // (  平均分,(movieId,总分，打分次数,电影名))
      .sortByKey(false) //按平均分排序
      .map(x => (x._2._1, x._2._4.substring(0, x._2._4.indexOf(" (")), x._2._2, x._2._3, x._1)) //  ( movieId, 电影名,总分，打分次数，平均分)
      .take(10)
      .foreach(println)

    println("1. 按平均分计算最受男性喜欢的电影TOP10")
    maleFavoriteMovie.join(movieInfos) //   (   )
      .map(x => (x._2._1._3, (x._1, x._2._1._1, x._2._1._2, x._2._2))) // (  平均分,(movieId,总分，打分次数,电影名))
      .sortByKey(false) //按平均分排序
      .map(x => (x._2._1, x._2._4.substring(0, x._2._4.indexOf(" (")), x._2._2, x._2._3, x._1)) //  ( movieId, 电影名,总分，打分次数，平均分)
      .take(10)
      .foreach(println)






    sc.stop()
  }
}
