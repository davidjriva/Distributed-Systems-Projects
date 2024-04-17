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
    // val ca_joined_df = readAndJoinTrendsAndCategory(spark, "CA")
    // val ca_views_week_df = aggregateViewsPerWeek(ca_joined_df)
    // saveDataFrameAsCSV(ca_views_week_df, "/s/bach/l/under/driva/csx55/Term-Project/data/CA_week_data")

    // Reading in Great Britain Data (GB):
    // val gb_joined_df = readAndJoinTrendsAndCategory(spark, "GB")
    // val gb_views_week_df = aggregateViewsPerWeek(gb_joined_df)
    // saveDataFrameAsCSV(gb_views_week_df, "/s/bach/l/under/driva/csx55/Term-Project/data/GB_week_data")

    // Reading in United States Data (US):
    // val us_joined_df = readAndJoinTrendsAndCategory(spark, "US")
    // val us_views_week_df = aggregateViewsPerWeek(us_joined_df)
    // saveDataFrameAsCSV(us_views_week_df, "/s/bach/l/under/driva/csx55/Term-Project/data/US_week_data")

    // Reading in Russia Data (RU):
    // val ru_joined_df = readAndJoinTrendsAndCategory(spark, "RU")
    // val ru_views_week_df = aggregateViewsPerWeek(ru_joined_df)
    // saveDataFrameAsCSV(ru_views_week_df, "/s/bach/l/under/driva/csx55/Term-Project/data/RU_week_data")

    // Reading in Japan Data (JP):
    // val jp_joined_df = readAndJoinTrendsAndCategory(spark, "JP")
    // val jp_views_week_df = aggregateViewsPerWeek(jp_joined_df)
    // saveDataFrameAsCSV(jp_views_week_df, "/s/bach/l/under/driva/csx55/Term-Project/data/JP_week_data")

    // Fast run with week data in CSV already:
    val ca_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/CA_week_data/part-00000-11c94ac5-97e6-4dea-b3a8-2d5f2ec1118d-c000.csv")
    val gb_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/GB_week_data/part-00000-f4d2195b-ce14-48b0-8e61-ee2c1a327552-c000.csv")
    val us_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/US_week_data/part-00000-edf17ebf-85d2-4af0-bdab-fc358eacb879-c000.csv")
    val ru_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/RU_week_data/part-00000-a524d12b-6c2f-47b4-aff5-deb820f24530-c000.csv")
    val jp_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/JP_week_data/part-00000-49a20846-e447-4128-9456-39d3e55a9ed6-c000.csv")

    val location_ids = Array("ca", "gb", "us", "ru", "jp")
    val location_df_array: Array[DataFrame] = Array(ca_views_week_df, gb_views_week_df, us_views_week_df, ru_views_week_df, jp_views_week_df)


    val joined_week_df: DataFrame = joinWeekViewData(location_df_array, location_ids)
                                    .select("categoryTitle", "week_year", "ca_percent_change", "gb_percent_change", "us_percent_change", "ru_percent_change", "jp_percent_change") // drop unnecessary cols
    joined_week_df.show()

    // Extract volatile trends for all three locs
    location_ids.foreach(id => sortPercentChangesAndSave(joined_week_df, id + "_percent_change", "/s/bach/l/under/driva/csx55/Term-Project/data/ordered_" + id +"_percent_data"))

    spark.stop()
  }

  def readAndJoinTrendsAndCategory(spark: SparkSession, country_code: String): DataFrame = {
    val trends_df: DataFrame = spark.read.option("header", "true").csv("file:///" + "/s/bach/l/under/driva/csx55/Term-Project/data/" + country_code + "_youtube_trending_data.csv")
    val category_df: DataFrame = spark.read.option("header", "true").csv("file:///" + "/s/bach/l/under/driva/csx55/Term-Project/data/" + country_code + "_category_id.csv")
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

  def joinWeekViewData(dfs: Array[DataFrame], location_ids: Array[String]): DataFrame = {
    var ct = 0
    var joined_df = dfs.head
                      .withColumnRenamed("percent_change", location_ids(ct) + "_percent_change")
                      .withColumn("foreign_key", concat(col("week_year"), lit("-"), col("categoryTitle")))
    ct += 1

    dfs.tail.foreach(df => {
      val other_df = df
                      .withColumnRenamed("percent_change", location_ids(ct) + "_percent_change")
                      .withColumn("foreign_key", concat(col("week_year"), lit("-"), col("categoryTitle")))
                      .drop("categoryTitle")
                      .drop("week_year")

      joined_df = joined_df.join(other_df, Seq("foreign_key")) 
      ct += 1
    })

    joined_df
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

