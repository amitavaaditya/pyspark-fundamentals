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
    jsonDF = spark.read.csv(datasetURI, header=True, inferSchema=True).persist()
    jsonDF.printSchema()
    return jsonDF


def transform_actions_functional(jsonDF):
    """Tranformations/Actions to be carried out using functions"""
    # Select title, variety, country and winery
    jsonDF.select(jsonDF['title'], jsonDF['variety'], jsonDF['title'], jsonDF['country'], \
                  jsonDF['winery']).show()
    # Select title, variety, country and winery where country is Italy
    jsonDF.select(jsonDF['title'], jsonDF['variety'], jsonDF['title'], jsonDF['country'], \
                  jsonDF['winery']).where(jsonDF['country'] == 'Italy').show()
    

def transform_actions_sql(spark, jsonDF):
    """Tranformations/Actions to be carried out using Spark SQL"""
    # Create the temporary view winemag from the DataFrame
    jsonDF.createOrReplaceTempView('winemag')
    # Display top 20 countries with highest number of wine varieties
    spark.sql('SELECT country, COUNT(country) as count FROM winemag \
        GROUP BY country ORDER BY count DESC').show()


if __name__ == '__main__':
    # Create the Spark Session
    spark = create_spark_session()
    # Read the CSV dataset
    datasetURI = 'datasets/USvideos.csv'
    jsonDF = read(spark, datasetURI)
    # Perform Transformations/Actions using functions
    transform_actions_functional(jsonDF)
    # Perform Transformations/Actions using Spark SQL
    transform_actions_sql(spark, jsonDF)
