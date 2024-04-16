import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{udf, col, split, size, avg, lower, sum, explode, desc}

object MovieAnalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("MovieAnalysis").master("local").getOrCreate()
    
    // Reading in data
    val movies_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/hw4/data/movies.csv")
    val tags_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/hw4/data/tags.csv")
    val rating_df: DataFrame = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/hw4/data/ratings.csv")

    // Q1: How many movies were released for every year within the dataset?
    val extract_year = udf((movieTitle: String) => extractYear(movieTitle))
    val moviesWithYear_df = movies_df.withColumn("year", extract_year(col("title")))
    val moviesCountByYear = moviesWithYear_df
                              .groupBy("year")
                              .count()
                              .filter(col("year").isNotNull)
                              .orderBy("year")
    moviesCountByYear.show()

    // Q2: What is the average number of genres for movies within this dataset
    val avgGenres = movies_df
                      .withColumn("genres_split", split(col("genres"), "\\|"))
                      .withColumn("num_genres", size(col("genres_split")))
                      .agg(avg(col("num_genres")))
    avgGenres.show()

    /* Q3: Rank the genres in the order of their ratings? Again, a movie may span multiple genres; such a movie should be counted in all the genres

      Job Details: 
        1.) Joins the movies_df & rating_df via an inner join. In this job we only care to look at the genres and rating columns.
        2.) We next have to split the genres column based on the delimiter (|)
        3.) We then use the explode function to turn the array of genres into separate rows with a new row for each genre associated with a movie
        4.) The dataframes are then grouped by genre and the total rating for that genre is aggregated
        5.) You finally will order the total rating column to get the genres ranked in order of their rating
    */
    val joined_df = movies_df.join(rating_df, Seq("movieId"), "inner")
                    .select("genres", "rating")
                    .withColumn("genres_split", split(col("genres"), "\\|"))
                    .drop("genres")
                    .select(explode(col("genres_split")).as("individual_genre"), col("rating"))
                    .groupBy("individual_genre")
                    .agg(sum("rating").as("total_rating"))
                    .orderBy("total_rating")

    joined_df.show()

    // Q5: How many movies have been tagged as "comedy"
    val comedy_ct = tags_df
                      .select("tag")
                      .filter(lower(col("tag"))
                      .contains("comedy"))
                      .count()

    println(s"Comedy Count: $comedy_ct")

    //Q6. What are the different genres within this dataset? How many movies were released within different
    // genres? A movie may span multiple genres; in such cases, that movie should be counted in all the genres? 
    
    val separated_genres_df = movies_df
                              .select("genres")
                              .withColumn("genres_split", split(col("genres"), "\\|"))
                              .select(explode(col("genres_split")).as("individual_genre"))
                              .select("individual_genre")

    // Getting the distinct genres and counting how many movies are in each genre
    val genre_count_df = separated_genres_df
                          .groupBy("individual_genre")
                          .count()
                          .orderBy(desc("count"))
    genre_count_df.show()

    spark.stop()
  }

  def extractYear(movieTitle: String): Option[String] = {
    val pattern = "\\((\\d{4})\\)".r
    val year = pattern.findFirstMatchIn(movieTitle).map(_.group(1))
    year
  }
}

