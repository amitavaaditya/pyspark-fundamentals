#!/usr/bin/env python3

from pyspark import SparkConf, SparkContext


def create_spark_context():
    """Create the Spark Context"""
    conf = SparkConf().setMaster("local[*]").setAppName("RDD Reader")   # Set Pyspark config
    sc = SparkContext(conf=conf)    # Create SparkContext object for RDD operations
    return sc


def read(sc, datasetURI):
    """Read TXT dataset into an RDD"""
    txtRDD = sc.textFile(datasetURI).persist()
    return txtRDD


def transformation(txtRDD):
    """Tranformations to be carried out"""
    nonBlankLinesRDD = txtRDD.filter(lambda x: x.strip()).persist()
    blankLinesRDD = txtRDD.subtract(nonBlankLinesRDD)
    wordsRDD = nonBlankLinesRDD.flatMap(lambda x: x.split()).persist()
    wordCountRDD = wordsRDD.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
    return nonBlankLinesRDD, blankLinesRDD, wordsRDD, wordCountRDD


def action(txtRDD, nonBlankLinesRDD, blankLinesRDD, wordsRDD, wordCountRDD):
    """Actions to be carried out"""
    print('Total number of lines : {}'.format(txtRDD.count()))
    print('Number of non-blank lines : {}'.format(nonBlankLinesRDD.count()))
    print('Number of blank lines : {}'.format(blankLinesRDD.count()))
    print('Total number of words : {}'.format(wordsRDD.count()))
    print('Frequency of Words :')
    for word, count in wordCountRDD.collect():
        print('{} : {}'.format(word, count))



if __name__ == '__main__':
    # Create the Spark Context
    sc = create_spark_context()
    # Read the TXT dataset
    datasetURI = 'datasets/lorem_ipsum.txt'
    txtRDD = read(sc, datasetURI)
    # Perform Transformations
    nonBlankLinesRDD, blankLinesRDD, wordsRDD, wordCountRDD = transformation(txtRDD)
    # Perform Actions
    action(txtRDD, nonBlankLinesRDD, blankLinesRDD, wordsRDD, wordCountRDD)
