package yc74

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
需求6: 分析 不同类型的电影总数
输出格式:   ( 类型,数量)
*/

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Test6 {
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

    //需求6: 分析 不同类型的电影总数
    //输出格式:   ( 类型,数量)

    //Animation|Children's|Comedy
    // -> 切分  1: [Animation,Children's,Comedy]          ????????????????            flatMapValues ->   [x,y,z]
    //               =>   (1,Animation), (1,Children's), (1,Comedy)
    //               => (类型名，数量1)  (Animation,1), (Children's,1), (Comedy,1)
    //           汇总
    moviesRDD.map(   _.split("::"))
        .map(   x=> (x(0),  x(2) ))      //    (   编号,类型)  =>   (1, Animation|Children's|Comedy )  =>
        .flatMapValues(  types=>{  types.split("\\|")  }  )     // (1,Animation), (1,Children's), (1,Comedy)
        .map(   x=>(x._2,1) )
        .reduceByKey(   (x,y)=> (x+y) )
        .foreach( println )




    sc.stop()
  }
}
