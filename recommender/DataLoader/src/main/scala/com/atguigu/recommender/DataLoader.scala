package com.atguigu.recommender


import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * 商品数据集
 * 3982 商品id
 * ^Fuhlen 富勒 M8眩光舞者时尚节能无线鼠标(草绿)(眩光.悦动.时尚炫舞鼠标 12个月免换电池 高精度光学寻迹引擎 超细微接收器10米传输距离) 商品名称
 * ^1057,439,736 商品分类id
 * ^B009EJN4T2 亚马逊id
 * ^https://images-cn-4.ssl-images-amazon.com/images/I/31QPvUDNavL._SY300_QL70_.jpg 商品图片
 * ^外设产品|鼠标|电脑/办公 标签
 * ^富勒|鼠标|电子产品|好用|外观漂亮 ugc
 */

case class Product(productId: Int, name: String,imageUrl:String, categories: String, tags: String)

/**
 * 评分数据集
 * 4867, 用户id
 * 457976, 商品id
 * 5.0, 用户评分
 * 1395676800 时间戳
 */

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

/**
 * MongoDB 链接配置
 */
case class MongoConfig(uri: String, db: String)

object DataLoader {

  //定义数据文件路径
  val PRODUCT_DATA_PATH = "/Users/hycao/IdeaProjects/ECommerceRecommendSystem/recommender/DataLoader/src/main/resources/products.csv"
  val RATING_DATA_PATH = "/Users/hycao/IdeaProjects/ECommerceRecommendSystem/recommender/DataLoader/src/main/resources/ratings.csv"

  //定义mongo中的数据存储表
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"


  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建spark conf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 加载数据
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map(item => {
      // product 数据通过^符号分割
      var attr = item.split("\\^")
      // 转换为product类
      Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()
    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    //作为下面方法的隐式参数，免得每次都调用
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 存入mongoDB
    storeDataInMongoDB(productDF, ratingDF)

    spark.stop()
  }
  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit={
    // 新建一个mongo链接客户端
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 定义要操作的表
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    // 如果表存在则删掉
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //将当前数据存入对应表中
    productDF.write.option("uri", mongoConfig.uri).option("collection",MONGODB_PRODUCT_COLLECTION).mode("overwrite").format("com.mongodb.spark.sql")
      .save()
    ratingDF.write.option("uri", mongoConfig.uri).option("collection",MONGODB_RATING_COLLECTION).mode("overwrite").format("com.mongodb.spark.sql")
      .save()

    // 对表创建索引

    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))

    mongoClient.close()
  }
}
