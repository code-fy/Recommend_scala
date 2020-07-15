package com.atguigustatistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"

  // 定义mongodb中统计后的存储表
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_RECENTLY_PRODUCTS = "RateRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {

    var config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")

    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //  创建rating临时表
    ratingDF.createOrReplaceTempView("ratings")

    // 历史热门数据
    val rateMoreProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId order by count desc")
    storDFInMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)

    // 近期热门数据，把时间戳转换成YYYYMM
    // 创建日期转换工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册udf 将时间戳转换为年月
    spark.udf.register("changeDate",(x : Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    // 把原始的数据转换成想要的结构productID，score，yearMonth
    val ratingOfYearMonth = spark.sql("select productId, score, changeDate(timestamp) as yearMonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF = spark.sql("select productId, count(productId) as count, yearMonth from ratingOfMonth group by yearMonth ,productId order by yearMonth desc, count desc")
    storDFInMongoDB(rateMoreRecentlyProductsDF, RATE_RECENTLY_PRODUCTS)

    // 优质商品统计
    val avgerageProducts = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    storDFInMongoDB(avgerageProducts, AVERAGE_PRODUCTS)

    spark.stop()

  }

  // 将处理好的数据存入mongodb函数
  def storDFInMongoDB(df:DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig):Unit ={
    // 如果表存在则删掉
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collectionName)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }
}
