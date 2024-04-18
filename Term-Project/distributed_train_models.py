import pandas as pd
import os
from datetime import datetime, timedelta
from fbprophet import Prophet
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct, col, to_date, concat, lit
from pyspark.sql.types import FloatType, StructField, StructType, StringType, TimestampType

def train_and_forecast(column, prophet_df):
    # Convert the Spark DataFrame to a Pandas DataFrame
    prophet_pd = prophet_df.toPandas()
    
    # Fix the dates
    prophet_pd['year_month_day'] = prophet_pd['week_year'].apply(lambda x: datetime.strptime(x + '-1', "%Y-%W-%w"))
    prophet_pd['year_month_day'] = prophet_pd['year_month_day']
    prophet_pd = prophet_pd[['year_month_day', 'y']]
    prophet_pd.rename(columns={'year_month_day' : 'ds'}, inplace=True)
    
    # Train the model
    m = Prophet(weekly_seasonality=True, daily_seasonality=True)
    m.fit(prophet_pd)
    
    # Make a forecast
    future = m.make_future_dataframe(periods=365)
    forecast = m.predict(future)
    
    # Write the forecast to CSV
    output_dir = "/s/bach/l/under/driva/csx55/Term-Project/data/prophet_forecasts/"
    forecast.to_csv(os.path.join(output_dir, column + ".csv"), header=True, index=False)

def read_csv_and_forecast(spark, category_title):
    joined_df = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/joined_week_data/joined_week_data.csv")
    joined_df = joined_df.filter(col("categoryTitle") == category_title)
    joined_df = joined_df.drop("categoryTitle")

    for column in joined_df.columns[2:]:
        # Get the column and dates for training
        prophet_df = joined_df.select("week_year", column).withColumnRenamed(column, "y")

        # Train and forecast for each column in parallel
        train_and_forecast(column, prophet_df) 

def main():
    # Instantiate a session
    spark = SparkSession.builder.appName("Training FB Prophet Distributed").master("local").config("spark.executor.memory", "4g").config("spark.driver.memory", "4g").config("spark.executor.memoryOverhead", "1g").getOrCreate()

    # The locations & category we are modeling
    category_title = "Education"

    read_csv_and_forecast(spark, category_title)

if __name__ == "__main__":
    main()