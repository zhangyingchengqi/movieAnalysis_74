package yc74

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求：
 *1. 分析男性用户最喜欢看的前10部电影
 *2. 女性用户最喜欢看的前10部电影
 * 输出格式:  ( movieId, 电影名,总分，打分次数，平均分)
 */
/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Test4 {
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


    //1. 分析男性用户最喜欢看的前10部电影
    val ratings = ratingsRDD.map(_.split("::")) //UserID::MovieID::Rating::Timestamp
      .map(x => (x(0), (x(1), x(2)))) //  (UserID, (MovieID,Rating) )
    ratings.cache()
    val users = usersRDD.map(_.split("::")) //UserID::Gender::Age::OccupationID::Zip-code
      .map(x => (x(0), x(1))) //  (  UserID,Gender)
    users.cache()

    //将 ratings和   usersRDD 的数据合并
    val ratingwithGender = ratings.join(users) //   (  UserID,  ( (MovieID,Rating),Gender)  )
      .map(item => (item._1, item._2._1._1, item._2._1._2, item._2._2)) //  (  UserID,  MovieID,Rating,Gender  )

    //取出女性打分   (  UserID,  MovieID,Rating,Gender  )
    val femaleRatings = ratingwithGender.filter(item => item._4.equals("F"))
    femaleRatings.cache()
    //取出女性打分   (  UserID,  MovieID,Rating,Gender  )
    val maleRatings = ratingwithGender.filter(item => item._4.equals("M"))
    maleRatings.cache()

    println("女性评分总分最高的10部电影, 格式：  总评分,电影ID")
    femaleRatings.map(x => (x._2, x._3.toDouble)) //  ( MovieID,Rating )
      .reduceByKey((x, y) => x + y) //    ( MovieID,总评分 )
      .map(item => (item._2, item._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)

    println("男性评分总分最高的10部电影, 格式：  总评分,电影ID")
    maleRatings.map(x => (x._2, x._3.toDouble)) //  ( MovieID,Rating )
      .reduceByKey((x, y) => x + y) //    ( MovieID,总评分 )
      .map(item => (item._2, item._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)

    //改进版:  累计总分后计算平均分，按平均分排序，显示top 10,显示电影名
    val femaleRatingsDetail = femaleRatings.map(x => (x._2, (x._3.toDouble, 1))) //  ( MovieID,  (Rating,1)  )
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //   ( MovieID,(总评分,总次数 ) )
      .map(x => (x._1, (x._2._1, x._2._2, x._2._1 / x._2._2))) //   ( MovieID,(总评分,总次数,平均分) )

    val maleRatingsDetail = maleRatings.map(x => (x._2, (x._3.toDouble, 1))) //  ( MovieID,  (Rating,1)  )
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //   ( MovieID,(总评分,总次数 ) )
      .map(x => (x._1, (x._2._1, x._2._2, x._2._1 / x._2._2))) //   ( MovieID,(总评分,总次数,平均分) )

    val movieInfos = moviesRDD.map(x => x.split("::")) // MovieID::Title::Genres
      .map(x => (x(0), (x(1), x(2)))) //   (MovieID,(Title,Genres))

    println("女性评分平均分最高的10部电影的详情")
    femaleRatingsDetail.join(movieInfos) //  ( MovieID,  ( (总评分,总次数,平均分), (Title,Genres) )
      .map(x => (x._2._1._3, (x._1, x._2._1._1, x._2._1._2, x._2._2._1, x._2._2._2))) //( 平均分,(MovieID, 总评分,总次数, Title,Genres ) )
      .sortByKey(false)
      .take(10)
      .foreach(println)


    println("男性评分平均分最高的10部电影的详情")
    maleRatingsDetail.join(movieInfos) //  ( MovieID,  ( (总评分,总次数,平均分), (Title,Genres) )
      .map(x => (x._2._1._3, (x._1, x._2._1._1, x._2._1._2, x._2._2._1, x._2._2._2))) //( 平均分,(MovieID, 总评分,总次数, Title,Genres ) )
      .sortByKey(false)
      .take(10)
      .foreach(println)


    sc.stop()
  }
}
