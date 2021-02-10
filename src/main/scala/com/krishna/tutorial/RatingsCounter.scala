package com.krishna.tutorial

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc: SparkContext = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines: RDD[String] = sc.textFile("./src/main/resources/MoviesDatabase.data")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings: RDD[String] = lines.map(x => x.split("\t")(2))

    // Count up how many times each value (rating) occurs
    val results: collection.Map[String, Long] = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults: Seq[(String, Long)] = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}
