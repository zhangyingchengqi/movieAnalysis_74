package yc74

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求：
 * 需求5: 分析最受不同年龄段人员欢迎的电影的前10
 * 输出格式:   ( movieId, 电影名,平均分)
 * *
 */
/** 需求5: 分析最受不同年龄段人员欢迎的电影的前10
 * 原始数据集中的年龄段划分
 * under 18: 1
 * 18 - 24: 18
 * 25 - 34: 25
 * 35 - 44: 35
 * 45 - 49: 45
 * 50 - 55: 50
 * 56 + 56
 *
 */

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Test5 {
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


    var age = "1" //18岁以下的人喜好的电影top10
    println("年龄段为:" + age + "喜爱的电影Top 10:")
    //1. 筛选人  age=1
    val userWithinAge = usersRDD.map(_.split("::"))
      .map(x => (x(0), x(2)))
      .filter(_._2.equals(age)) //   (    UserId, age )
    userWithinAge.cache()
    println("年龄为1的用户量:" + userWithinAge.count())
    //解决方案:    1. userWithinAge.collect()  ->    broadcast
    //       2. 做成两个RDD    ,然后join

    val ratings = ratingsRDD.map(_.split("::")) // UserID::MovieID::Rating::Timestamp
      .map(x => {
        (x(0), (x(1), x(2)))
      }) //  (UserID,( MovieID,Rating) )
    ratings.cache()
    println("评分数据量为:" + ratings.count())

    //join
    val ratingWithinAge = userWithinAge.join(ratings) //   (     UserId,  ( age,( MovieID,Rating)  )
      .map(item => (item._2._2._1, item._2._1, item._1, item._2._2._2)) // (        MovieID,  age, userid,Rating  )

    println("年龄在1的评分数据量为:" + ratingWithinAge.count())

    //计算平均分
    ratingWithinAge.map(x => (x._1, (x._4.toDouble, 1))) //  (  MovieID, (rating,1) )
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // ( MovieID,  (  总分,总次数)
      .map(item => (item._2._1 / item._2._2, (item._1, item._2._1, item._2._2))) // (  平均分,(电影id, 总分数,次数) )
      .sortByKey(false)
      .take(10)
      .foreach(println)

    // 扩展:  上面的显示中加入电影信息   电影名,类型.
    println("年龄段为:" + age + "喜爱的电影Top 10:( movieId, 电影名,平均分,总分数,观影次数,类型)") //   ratingWithinAge:        (        MovieID,  age, userid,Rating  )
    val rdd = ratingWithinAge.map(x => (x._1, (x._4.toDouble, 1))) //  (  MovieID, (rating,1) )
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // ( MovieID,  (  总分,总次数)
      .map(item => (item._1, (item._2._1 / item._2._2, item._2._1, item._2._2))) // ( 电影id ,(平均分, 总分数,次数) )


    val movieInfos = moviesRDD.map(x => x.split("::"))
      .map(x => (x(0), (x(1), x(2)))) //   (  movieid, (title,genre) )

    val joinedRdd = rdd.join(movieInfos) //  (  movieId,((平均分, 总分数,次数),(title,genre) ) )
    joinedRdd.cache()


    joinedRdd.map(x => (x._2._1._1, (x._1, x._2._2._1, x._2._1._2, x._2._1._3, x._2._2._2))) // (  平均分,(movieId,电影名,总分数,次数,genres))
      .sortByKey(false) //按平均分排序
      .map(x => (x._2._1, x._2._2.substring(0, x._2._2.indexOf(" (")), x._1, x._2._3, x._2._4, x._2._5)) //  ( movieId, 电影名,平均分,总分数,次数,genres)
      .take(10)
      .foreach(println)

    //扩展二：  二次排序，先根据平均分排，如平均分相同，按观影次数排序(降),观影次数相同(降)，则按电影名排序(升)
    println("二次排序，先根据平均分排，如平均分相同，按观影次数排序(降),观影次数相同(降)，则按电影名排序(升)")
    joinedRdd.sortBy(item => (-item._2._1._1, -item._2._1._3, item._2._2._1)) // tuple       (  movieId,((平均分, 总分数,次数),(title,genre) ) )
      .map(x => (x._1, x._2._2._1, x._2._2._2, x._2._1._1, x._2._1._2, x._2._1._3))
      .take(10)
      .foreach(println)

    //扩展三: 是否可以利用广播变量完成操作
    println("年龄在" + age + "的用户数据量:" + userWithinAge.count()) // 222个用户
    val userArray = userWithinAge.collect() //    (userid,age)
    val broadcastRef = sc.broadcast(userArray)
    val ageRef = sc.broadcast(age)

    //  (UserID,( MovieID,Rating) )
    val ratingWithAgeRdd = ratings.filter(rate => {
      val userArray = broadcastRef.value
      val age = ageRef.value
      userArray.contains((rate._1, age))
    })

    println("用户的打分数据量有:" + ratingWithAgeRdd.count()) //27211

    ratingWithAgeRdd.map(item => (item._2._1, (item._2._2.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //   (   movieid, ( 总分，总次数 ))
      .map(item => (item._1, item._2._1, item._2._2, item._2._1 / item._2._2)) //  (   movieid,  总分，总次数 ,平均分)
      .sortBy(item => (-item._4, -item._3, item._1))
      .take(100)
      .foreach(println)


    sc.stop()
  }
}
