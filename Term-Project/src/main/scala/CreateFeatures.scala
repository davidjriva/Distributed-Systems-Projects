import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.ml.stat.ANOVATest
import scala.collection.mutable.ArrayBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object CreateFeatures {
  def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("TermProjectTest").master("local").getOrCreate()
        val countries: Array[String] = Array("GB", "RU", "JP", "BR", "DE", "IN")
        
        // UDFs to calculate features
        val engagementRateUDF = udf((likes: Int, dislikes: Int, comments: Int, views: Int) =>
            (likes + dislikes + comments).toDouble / (views + 1e-6)
        )

        val likeDislikeRatioUDF = udf((likes: Int, dislikes: Int) =>
            likes.toDouble / (dislikes + 1e-6)
        )

        val commentViewRatioUDF = udf((comments: Int, views: Int) =>
            comments.toDouble / (views + 1e-6)
        )

        val dislikesPerCommentUDF = udf((dislikes: Int, comments: Int) =>
            dislikes.toDouble / (comments + 1e-6)
        )

        val daysSincePublicationUDF = udf((trendingDate: String, publishedAt: String) => {
            val trendingFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            val publishedAtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            val trendingDateObj = LocalDate.parse(trendingDate, trendingFormatter)
            val publishedAtObj = LocalDate.parse(publishedAt, publishedAtFormatter)
            (trendingDateObj.toEpochDay - publishedAtObj.toEpochDay).toInt
        })

        val likesPerDayUDF = udf((likes: Int, daysSincePublication: Int) =>
            likes.toDouble / (daysSincePublication + 1e-6)
        )

        val commentsPerDayUDF = udf((comments: Int, daysSincePublication: Int) =>
            comments.toDouble / (daysSincePublication + 1e-6)
        )

        val viewVelocityUDF = udf((viewCount: Int, daysSincePublication: Int) =>
            math.log(viewCount + 1) / (daysSincePublication + 1)
        )

        countries.foreach(country_code => {
            // Read in CSV
            val trends_df: DataFrame = spark.read.option("header", "true").csv("file:///" + "/s/bach/l/under/driva/csx55/Term-Project/data/youtube_trends_data/" + country_code + "_youtube_trending_data.csv")
            val category_df: DataFrame = spark.read.option("header", "true").csv("file:///" + "/s/bach/l/under/driva/csx55/Term-Project/data/category_ids/" + country_code + "_category_id.csv")
                                        .withColumnRenamed("title", "categoryTitle")

            val joined_df = trends_df
                .join(category_df, trends_df("categoryId") === category_df("id"), "inner")
                .drop("id")
                .withColumn("view_count", col("view_count").cast("int")) // Turn col to integer
                .withColumn("publishedAt", to_date(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) // Parse ISO 8601 format
                .orderBy(desc("view_count")) // Sort in descending order based on views

            // Calculate necessary features
            val feature_df = joined_df
                                .withColumn("engagement_rate", engagementRateUDF(col("likes"), col("dislikes"), col("comment_count"), col("view_count")))
                                .withColumn("like_dislike_ratio", likeDislikeRatioUDF(col("likes"), col("dislikes")))
                                .withColumn("comment_view_ratio", commentViewRatioUDF(col("comment_count"), col("view_count")))
                                .withColumn("dislikes_per_comment", dislikesPerCommentUDF(col("dislikes"), col("comment_count")))
                                .withColumn("days_since_publication", daysSincePublicationUDF(col("trending_date"), col("publishedAt")))
                                .withColumn("likes_per_day", likesPerDayUDF(col("likes"), col("days_since_publication")))
                                .withColumn("comments_per_day", commentsPerDayUDF(col("comment_count"), col("days_since_publication")))
                                .withColumn("view_velocity", viewVelocityUDF(col("view_count"), col("days_since_publication")))

            feature_df.show()
            // Replace missing values with median
            val median_dislikes_per_comment = feature_df.stat.approxQuantile("dislikes_per_comment", Array(0.5), 0.0)(0)
            val median_comments_per_day = feature_df.stat.approxQuantile("comments_per_day", Array(0.5), 0.0)(0)
            val median_view_velocity = feature_df.stat.approxQuantile("view_velocity", Array(0.5), 0.0)(0)

            // Replace missing values with the median
            val filled_df = feature_df
                                .na.fill(median_dislikes_per_comment, Seq("dislikes_per_comment"))
                                .na.fill(median_comments_per_day, Seq("comments_per_day"))
                                .na.fill(median_view_velocity, Seq("view_velocity"))
                                .drop("description", "thumbnail_link", "comments_disabled", "ratings_disabled")

            saveDataFrameAsCSV(filled_df, f"/s/bach/l/under/driva/csx55/Term-Project/data/model_feature_data/" + country_code + "_data")
        })
  }

    def saveDataFrameAsCSV(df: DataFrame, outputPath: String): Unit = {
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(outputPath)
    }
}