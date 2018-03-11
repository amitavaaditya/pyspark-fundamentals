##### Application of Spark RDD & DataFrame APIs using Pyspark

This repository demonstrates sample applications of the Pyspark RDD & DataFrame APIs to parse and process different types of datasets.

This was written in Python v3.6.4 using Pyspark v2.3 library.

The description of file names & directories are as follows:
- **datasets** This folder contains the different datasets used
- **CsvDfProcessing.py** This file shows processing of CSV files using Pyspark SQL DataFrame API
- **JsonDfProcessing.py** This file shows processing of JSON files using Pyspark SQL DataFrame API
- **RDDProcessing.py** This file shows processing of plaintext files using Pyspark RDD API

The files can be executed in one of three ways:
- By making the script executable (chmod +x <filename.py>) and executing the file directly from terminal (./<filename.py>)
- By running the file as a regular python script by invoking the python interpreter (python <filename.py>
- By running the script using spark-submit from the terminal (spark-submit <filename.py>)

**Note:** The spark config is hardcoded to run on a local standalone spark cluster with all available cores (local[*]). Use --master flag along with spark-sumbit to override the same.
