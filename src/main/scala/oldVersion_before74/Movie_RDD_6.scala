package oldVersion_before74

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 *  数据：
 *  1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 *  3，"movies.dat"：MovieID::Title::Genres
 *  4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Movie_RDD_6 {
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

    //需求6: 分析 不同类型的电影总数               MovieID::Title::Genres
    // MovieID::Title::Genres
    moviesRDD.map(x => x.split("::"))
             .map(x => (x(0), x(2)))
             .flatMapValues(mtype => {        // (f: String => TraversableOnce[U]): RDD[(String, U)]
        mtype.split("\\|")     //   Drama|War  =>   ["Drama","War"]    =>     [(电影名,"Drama"),(电影名,"War")]
      }).map(x => (x._2, 1))
      .reduceByKey((x, y) => (x + y))
      .foreach(println)

    sc.stop()
  }
}
