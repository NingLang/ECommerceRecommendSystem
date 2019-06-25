import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test {

    val PRODUCT_DATA_PATH = "D:\\xiangmu\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]"
        )
        // 创建一个spark config
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("Test")
        // 创建spark session
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
        val lineLengths = productRDD.map(s => s.length)

        val totalLength = lineLengths.reduce((a, b) => a + b)

        System.out.println(lineLengths)
        System.out.println(totalLength)

    }
}
