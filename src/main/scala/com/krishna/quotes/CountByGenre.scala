package com.krishna.quotes

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ DataFrame, SparkSession }

object CountByGenre extends App {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkSession using every core of the local machine, named GenreCounter
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("GenreCounter")
    .getOrCreate()


  val filePath = getClass.getResource("/Quotes.csv").getPath

  // Load up each line from the CSV database file
  val ddf: DataFrame = spark.read
    .format("csv")
    .option("header", "true") // Use first line of all files as header
    .option("delimiter", ";") // by default columns are delimited using ,
    .option("inferSchema", "true") // Automatically infer data types
    .option("mode", "PERMISSIVE")  //nulls are inserted for fields that could not be parsed correctly
    .load(filePath)

  val result = ddf.groupBy("genre").count()
  //result.

  //ddf.show(false)
  ddf.printSchema()
  result.foreach(println(_))
}
