# acmws-2020-pyspark
Repository for Pyspark Lab Session at ACM Winter School - 2020

## Prerequisite

Login to the IISc cluster (10.24.24.2) using SSH. Windows users can use Putty. Please use the username allocated for you in the following google sheet: The password is **acmws@iisc**

## Task 1 - Cluster Information

Visit http://10.24.24.2:8088 in your browser. Find the following information: Total memory, Total cores, Total active nodes, Memory per node, Cores per node and Scheduler type.

## Task 2 - Inspecting the dataset

Run the following *HDFS* commands one by one in your terminal to inspect the dataset and observe the output,

```
# List all the files
hdfs dfs -ls /ml/small 

# Print first few lines of the files
hdfs dfs -head /ml/small/movies.csv
hdfs dfs -head /ml/small/ratings.csv
hdfs dfs -head /ml/small/tags.csv
hdfs dfs -head /ml/small/links.csv
```

## Task 3 - Launching Pyspark

Once logged into the cluster, run the following command:
```
pyspark --master yarn --deploy-mode client --conf spark.yarn.archive=hdfs:///spark-libs.jar --num-executors 2 --executor-cores 3 --executor-memory 2g
```
and you should get an output similar to the following,
```
Python 2.7.5 (default, Apr 11 2018, 07:36:10)
[GCC 4.8.5 20150623 (Red Hat 4.8.5-28)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
2020-01-13 23:19:13 WARN  NativeCodeLoader:60 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
2020-01-13 23:19:15 WARN  Utils:66 - Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.0
      /_/

Using Python version 2.7.5 (default, Apr 11 2018 07:36:10)
SparkSession available as 'spark'.
>>>
```

## Task 4 - Caching dataset in Spark

Enter the following statements in *Pyspark* terminal to read and cache the datset in memory,
```
m = sc.textFile("hdfs:///ml/small/movies.csv").cache()
r = sc.textFile("hdfs:///ml/small/ratings.csv").cache()
t = sc.textFile("hdfs:////ml/small/tags.csv").cache()
l = sc.textFile("hdfs:///ml/small/links.csv").cache()
```

## Task 5 - How many distinct users have tagged movies? 
```
Will be added during lab session.
```

## Task 6 - How many users have both given ratings and tagged movies?
```
Will be added during lab session.
```

## Task 7 - What are the most number of genres assigned to any movie?
```
Will be added during lab session.
```

## Task 8 - Which are the genres with the most and least number of movies?
```
Will be added during lab session.
```

Exit the *Pyspark* terminal by entering `quit()`. 

## References

1. https://spark.apache.org/docs/2.4.0/rdd-programming-guide.html
2. http://spark.apache.org/docs/2.1.1/api/python/pyspark.html
3. http://files.grouplens.org/datasets/movielens/ml-latest-README.html,
