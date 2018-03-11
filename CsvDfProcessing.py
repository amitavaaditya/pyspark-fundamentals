#!/usr/bin/env python3


from pyspark.sql import SparkSession


def create_spark_session():
    """Create the Spark Session"""
    return SparkSession.builder \
                        .appName("Dataframe Reader") \
                        .master("local[*]") \
                        .getOrCreate()

def read(spark, datasetURI):
    """Read CSV dataset into an DataFrame and display its Schema"""
    csvDF = spark.read.csv(datasetURI, header=True, inferSchema=True).persist()
    csvDF.printSchema()
    return csvDF


def transform_actions_functional(csvDF):
    """Tranformations/Actions to be carried out using functions"""
    # Select title, channel title, likes and dislikes
    csvDF.select(csvDF.title, csvDF.channel_title, csvDF.likes, csvDF.dislikes).show()
    # Select title, channel name, likes and dislikes where channel title is iJustine
    csvDF.select(csvDF.title, csvDF.channel_title, csvDF.likes, csvDF.dislikes)\
        .where(csvDF.channel_title == 'iJustine').show()
    

def transform_actions_sql(spark, csvDF):
    """Tranformations/Actions to be carried out using Spark SQL"""
    # Create the temporary view USVideos from the DataFrame
    csvDF.createOrReplaceTempView('USvideos')
    # Display channels with count of videos sorted in descending order of count
    spark.sql('SELECT channel_title, COUNT(channel_title) as count FROM USvideos \
        GROUP BY channel_title ORDER BY count DESC').show()


if __name__ == '__main__':
    # Create the Spark Session
    spark = create_spark_session()
    # Read the CSV dataset
    datasetURI = 'datasets/USvideos.csv'
    csvDF = read(spark, datasetURI)
    # Perform Transformations/Actions using functions
    transform_actions_functional(csvDF)
    # Perform Transformations/Actions using Spark SQL
    transform_actions_sql(spark, csvDF)
