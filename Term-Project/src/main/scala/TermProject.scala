import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, col, to_date, sum, weekofyear, concat, lit, date_format, lag, collect_list, expr}
import org.apache.spark.ml.stat.ANOVATest
import scala.collection.mutable.ArrayBuffer

object TermProject {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("TermProjectTest").master("local").getOrCreate()
    val regions: Array[String] = Array("CA", "GB", "US", "RU", "JP", "BR", "DE", "FR", "IN", "KR", "MX")

    // Pt.1: Read in and save dataframes with view count per week:
    // Define the directory containing the CSV files
    regions.foreach(region => {
      val joined_df = readAndJoinTrendsAndCategory(spark, region)
      val views_weeks_df = aggregateViewsPerWeek(joined_df)
      saveDataFrameAsCSV(views_weeks_df, f"/s/bach/l/under/driva/csx55/Term-Project/data/week_data/" + region + "_week_data.csv")
    })

    // Read in view per week data from CSV files:
    val ca_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/CA_week_data/part-00000-ab3d8d64-1316-422a-adcf-48ec4b8b3c09-c000.csv")
    val gb_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/GB_week_data/part-00000-77614d1b-1454-47e8-90a6-6c3e128a9282-c000.csv")
    val us_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/US_week_data/part-00000-3a519b4e-0294-44e1-b824-08c9ec0d6eee-c000.csv")
    val ru_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/RU_week_data/part-00000-1315a3b5-4b67-48f9-8bc4-55c5eb908928-c000.csv")
    val jp_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/JP_week_data/part-00000-8b73b52d-b733-4842-8947-b1bd271d60f1-c000.csv")
    val br_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/BR_week_data/part-00000-9fccd3d4-5bbe-4143-92bc-2b09b697daf7-c000.csv")
    val de_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/DE_week_data/part-00000-a6f906aa-1785-4f23-88ef-053b6873ef6e-c000.csv")
    val fr_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/FR_week_data/part-00000-4c1df873-6649-4951-a592-594d347d4657-c000.csv")
    val in_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/IN_week_data/part-00000-6c364cc4-6e6e-4c2f-b228-a2ba34feee2b-c000.csv")
    val kr_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/KR_week_data/part-00000-bd89ed73-8804-4011-99ba-8e22bdc59a19-c000.csv")
    val mx_views_week_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/week_data/MX_week_data/part-00000-e7adbc99-342b-404e-9b0d-e6e99931f928-c000.csv")

    val location_df_array: Array[DataFrame] = Array(ca_views_week_df, gb_views_week_df, us_views_week_df, 
                                                    ru_views_week_df, jp_views_week_df, br_views_week_df,
                                                    de_views_week_df, fr_views_week_df, in_views_week_df, 
                                                    kr_views_week_df, mx_views_week_df)

    val cols_to_select: ArrayBuffer[String] = ArrayBuffer("categoryTitle", "week_year")
    regions.foreach(region => cols_to_select.append(region + "_percent_change"))
    
    val selected_cols = cols_to_select.map(col)
    val joined_week_df: DataFrame = joinWeekViewData(location_df_array, regions)
                                    .select(selected_cols: _*)
    saveDataFrameAsCSV(joined_week_df, "/s/bach/l/under/driva/csx55/Term-Project/data/percent_joined/")
    joined_week_df.show()

    // Extract volatile trends for all three locs
    regions.foreach(id => sortPercentChangesAndSave(joined_week_df, id + "_percent_change", "/s/bach/l/under/driva/csx55/Term-Project/data/percent_ordered_data/" + id + "_percent_data_ordered"))

    spark.stop()
  }

  def readAndJoinTrendsAndCategory(spark: SparkSession, country_code: String): DataFrame = {
    val trends_df: DataFrame = spark.read.option("header", "true").csv("file:///" + "/s/bach/l/under/driva/csx55/Term-Project/data/youtube_trends_data/" + country_code + "_youtube_trending_data.csv")
    val category_df: DataFrame = spark.read.option("header", "true").csv("file:///" + "/s/bach/l/under/driva/csx55/Term-Project/data/category_ids/" + country_code + "_category_id.csv")
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

