package yc74

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
需求9: 分析不同职业对观看电影类型的影响
格式:  (职业名,(电影类型,观影次数))
*/

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Test9 {
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

    moviesRDD.cache()

    // 需求9: 分析不同职业对观看电影类型的影响
    //格式:  (职业名,(电影类型,观影次数))
    //用户与职位
    val user = usersRDD.map(_.split("::")) //UserID::Gender::Age::OccupationID::Zip-code
      .map(x => (x(3), x(0))) //   (OccupationID,UserID)
    val rating = ratingsRDD.map(_.split("::"))
      .map(x => (x(0), x(1))) // (  userid, movieid)
    val occ = occupationsRDD.map(_.split("::"))
      .map(x => (x(0), x(1))) //   (  OccupationID,OccupationName)
    //合并用户与职业
    val uoRDD = user.join(occ) //  (  OccupationID, ( UserID,OccupationName )
      .map(item => (item._2._1, item._2._2)) // ( UserID,OccupationName )
    //电影与电影类型
    val moviesTypeRDD = moviesRDD.map(_.split("::"))
      .map(x => (x(0), x(2))) //    (   编号,类型)  =>   (1, Animation|Children's|Comedy )  =>
      .flatMapValues(types => {
        types.split("\\|")
      }) // (编号,Animation), (编号,Children's), (编号,Comedy)


    val rdd = uoRDD.join(rating) //  ( UserID, ( OccupationName, movieid) )
      .map(item => (item._2._2, item._2._1)) // ( movieid, OccupationName)
      .join(moviesTypeRDD) //  ( movieid, ( OccupationName, Animation ) )
      .map(item => (item._2._1, (item._1, item._2._2))) // (OccupationName, (movieid, Animation ) )

    rdd.groupByKey() // (  OccupationName,   Iterable[   (1, Animation ),(1, Animation ),(1, Animation ),(2, Animation ),(2, Animation ) ]
      .flatMapValues(array => {
        var A: Map[String, Int] = Map()
        array.foreach(item => {
          if (A.contains(item._2)) {
            var oldcount = A.getOrElse(item._2, 0) + 1
            A += (item._2 -> oldcount)
          } else {
            A += (item._2 -> 1)
          }
        })
        A
      })
      .collect()
      .foreach(println)


    sc.stop()
  }

}
