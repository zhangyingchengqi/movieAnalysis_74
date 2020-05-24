package oldVersion_before74

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 *  数据：
 *  1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 *  3，"movies.dat"：MovieID::Title::Genres
 *  4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object Movie_RDD_9 {
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


    //需求9: 分析不同职业对观看电影类型的影响
    val userWithOccupation=usersRDD.map( x=> x.split("::"))
            .map( x=> (x(3), x(0))  )   // UserID::Gender::Age::OccupationID::Zip-code
    val ratingsRDD = sc.textFile(filepath + "ratings.dat")  // UserID::MovieID::Rating::Timestamp
                        .map( x=> x.split("::") )
                        .map( x=> (x(0),x(1)     ))         // userid: movieID

    val occupationsRDD = sc.textFile(filepath + "occupations.dat")   // OccupationID::OccupationName
                          .map(x=> x.split("::"))
                          .map( x=> (x(0),x(1)) )
    //先求出用户与职业
    val uoRDD=userWithOccupation.join(occupationsRDD)
                      .map( item=> (item._2._1,item._2._2) )    // (5970,lawyer)
                      //.foreach(println)
    //再求出电影与类型    MovieID::Title::Genres
    val moviesTypeRDD=moviesRDD.map(x => x.split("::"))
            .map(x => (x(0), x(2)))
            .flatMapValues(mtype => {
                mtype.split("\\|")
             }).map(x => (x._1,x._2))       //  (电影编号,类型)
            // .foreach( println )
    //在ratings 中加入两个信息
    val r2RDD=ratingsRDD.join(uoRDD)          // (1489,(2395,artist))      用户编号,(电影编号,职业)
              .map(   item => ( item._2._1, (item._2._2, item._1 )  )  )   //电影编号,(职业, 用户编号)
           //.foreach(println)
    //内联电影类型(2427,((sales/marketing,3208),War))
    r2RDD.join(  moviesTypeRDD )   // (电影编号,((职业,用户编号),电影类型 )
         .map(  item =>  (  item._2._1._1,     item._2._2   )  )
         .groupByKey()
       .flatMapValues(  array=> {
          var A:Map[String,Int] = Map()
          array.foreach( item => {
                if( A.contains(item) ){
                   var oldcount=A.getOrElse(item, 0)+1
                   // +=(1.修改)   键    值
                   A += (item -> oldcount)
                }else{
                   A += (item -> 1)
                }
          })
          A
      })
      .foreach(println );

    sc.stop()
  }
}
