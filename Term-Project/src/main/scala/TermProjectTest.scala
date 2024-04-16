import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, col, to_date, sum, weekofyear, concat, lit, date_format, lag, collect_list, expr}
import org.apache.spark.ml.stat.ANOVATest

object TermProjectTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("TermProjectTest").master("local").getOrCreate()

    // Reading in Canada data (CA):
    // val ca_trends_path = "/s/bach/l/under/driva/csx55/Term-Project/data/CA_youtube_trending_data.csv"
    // val ca_category_path = "/s/bach/l/under/driva/csx55/Term-Project/data/CA_category_id.csv"
    // val ca_joined_df = readAndJoinTrendsAndCategory(spark, ca_trends_path, ca_category_path)

    // val ca_views_week_df = aggregateViewsPerWeek(ca_joined_df)
    // saveDataFrameAsCSV(ca_views_week_df, "/s/bach/l/under/driva/csx55/Term-Project/data/CA_week_data")

    // Reading in Great Britain Data (GB):
    // val gb_trends_path = "/s/bach/l/under/driva/csx55/Term-Project/data/GB_youtube_trending_data.csv"
    // val gb_category_path = "/s/bach/l/under/driva/csx55/Term-Project/data/GB_category_id.csv"
    // val gb_joined_df = readAndJoinTrendsAndCategory(spark, gb_trends_path, gb_category_path)

    // val gb_views_week_df = aggregateViewsPerWeek(gb_joined_df)
    // saveDataFrameAsCSV(gb_views_week_df, "/s/bach/l/under/driva/csx55/Term-Project/data/GB_week_data")

    // Reading in United States Data (US):
    // val us_trends_path = "/s/bach/l/under/driva/csx55/Term-Project/data/US_youtube_trending_data.csv"
    // val us_category_path = "/s/bach/l/under/driva/csx55/Term-Project/data/US_category_id.csv"
    // val us_joined_df = readAndJoinTrendsAndCategory(spark, us_trends_path, us_category_path)

    // val us_views_week_df = aggregateViewsPerWeek(us_joined_df)
    // saveDataFrameAsCSV(us_views_week_df, "/s/bach/l/under/driva/csx55/Term-Project/data/US_week_data")

    // Fast run with week data in CSV already:
    val ca_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/CA_week_data/part-00000-11c94ac5-97e6-4dea-b3a8-2d5f2ec1118d-c000.csv")
    val gb_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/GB_week_data/part-00000-f4d2195b-ce14-48b0-8e61-ee2c1a327552-c000.csv")
    val us_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/US_week_data/part-00000-edf17ebf-85d2-4af0-bdab-fc358eacb879-c000.csv")

    // Prefix with something unique prior to joining all three
    // Create foreign key to match up week-year and categoryTitle
    val caPrefixed_df = ca_views_week_df
                        .withColumnRenamed("percent_change", "ca_percent_change")
                        .withColumn("foreign_key", concat(col("week_year"), lit("-"), col("categoryTitle")))

    val gbPrefixed_df = gb_views_week_df
                        .withColumnRenamed("percent_change", "gb_percent_change")
                        .withColumn("foreign_key", concat(col("week_year"), lit("-"), col("categoryTitle")))
                        .drop("categoryTitle")
                        .drop("week_year")

    val usPrefixed_df = us_views_week_df
                        .withColumnRenamed("percent_change", "us_percent_change")    
                        .withColumn("foreign_key", concat(col("week_year"), lit("-"), col("categoryTitle")))
                        .drop("categoryTitle")
                        .drop("week_year")

    val joined_week_df: DataFrame = caPrefixed_df
                                    .join(gbPrefixed_df, Seq("foreign_key"))
                                    .join(usPrefixed_df, Seq("foreign_key"))
                                    .select("categoryTitle", "week_year", "ca_percent_change", "gb_percent_change", "us_percent_change") // drop unnecessary cols
    joined_week_df.show()

    // Extract volatile trends for all three locs
    sortPercentChangesAndSave(joined_week_df, "ca_percent_change", "/s/bach/l/under/driva/csx55/Term-Project/data/ordered_ca_percent_data")
    sortPercentChangesAndSave(joined_week_df, "gb_percent_change", "/s/bach/l/under/driva/csx55/Term-Project/data/ordered_gb_percent_data")
    sortPercentChangesAndSave(joined_week_df, "us_percent_change", "/s/bach/l/under/driva/csx55/Term-Project/data/ordered_us_percent_data")

    spark.stop()
  }

  def readAndJoinTrendsAndCategory(spark: SparkSession, trends_path: String, category_path: String): DataFrame = {
    val trends_df: DataFrame = spark.read.option("header", "true").csv("file:///" + trends_path)
    val category_df: DataFrame = spark.read.option("header", "true").csv("file:///" + category_path)
                                  .withColumnRenamed("title", "categoryTitle")

    val joined_df = trends_df
                    .join(category_df, trends_df("categoryId") === category_df("id"), "inner")
                    .drop("id")
                    .withColumn("view_count", col("view_count").cast("int")) // Turn col to integer
                    .withColumn("publishedAt", to_date(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) // Parse ISO 8601 format
                    .orderBy(desc("view_count")) // Sort in descending order based on views

    joined_df
  }

  def aggregateViewsPerWeek(joined_df: DataFrame): DataFrame = {
    // Extract week number from the date
    val totalViews_df = joined_df
                    .withColumn("week_number", weekofyear(col("publishedAt")))
                    .withColumn("year", date_format(col("publishedAt"), "yyyy"))
                    .withColumn("week_year", concat(col("year"), lit("-"), col("week_number")))
                    .groupBy("categoryTitle", "week_year")
                    .agg(sum("view_count")
                    .alias("total_views"))
                    .orderBy("week_year")

    // Use sliding window function to calculate week-to-week change in views per category
    val windowSpec = Window.partitionBy("categoryTitle").orderBy("week_year")
    val percentChange_df = totalViews_df
                    .withColumn("prev_week_views", lag("total_views", 1).over(windowSpec))
                    .withColumn("percent_change", (col("total_views") - col("prev_week_views")) / col("prev_week_views") * 100)
                    .orderBy(desc("percent_change"))

    percentChange_df
  }

  def sortPercentChangesAndSave(joined_week_df: DataFrame, percentName: String, savePath: String) : Unit = {
    val doubleName = percentName + "_double"
    val sorted_p_change_df = joined_week_df
      .select("categoryTitle", "week_year", percentName)
      .withColumn(doubleName, col(percentName).cast("Double"))
      .drop(percentName)
      .orderBy(desc(doubleName))

    saveDataFrameAsCSV(sorted_p_change_df, savePath)
  }

  def saveDataFrameAsCSV(df: DataFrame, outputPath: String): Unit = {
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(outputPath)
  }
}

