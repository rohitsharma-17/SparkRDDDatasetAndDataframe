package com.hashedin

import com.hashedin.bean.{AwardWinningTitlesCSV, AwardWinningTitlesJSON, DirectorListCSV, DirectorListJSON, MoviesCSV, MoviesJSON, NoOfEnglishPerYearCSV, NoOfEnglishPerYearJSON}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, count, first, max, struct, sum}

object Main {
  def main(args: Array[String]): Unit = {

    val csvFileInputPath = "/home/hasher/Downloads/imdb_movies.csv"
    val jsonFileInputPath = "/home/hasher/Downloads/imdb_movies.json"
    val fileOutputPath = "/home/hasher/Downloads/output/"
    val startYear = 1997
    val endYear = 2000

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark Session")
      .getOrCreate()

    // RDD
    val csvRDD = sparkSession.sparkContext.textFile(csvFileInputPath)
    val jsonRDD = sparkSession.sparkContext.textFile(jsonFileInputPath)
    val csvHeaderRDD = csvRDD.first();

    val englishLanguageRDD = csvRDD.filter(_.contains("English")).map(line => line.split("\t")(2).toInt).countByValue()
    englishLanguageRDD.foreach(println)

    val jsonEnglishLanguageRDD = jsonRDD.filter(_.contains("English")).map(_.replaceAll("[{}]", ""))
      .map(line => ((line.split("\",")(3)).split(":")(1)).replaceAll("\"", "").toLong).countByValue()
    jsonEnglishLanguageRDD.foreach(println)

    val awardWinningTitlesRDD = csvRDD.filter(_ != csvHeaderRDD).map(line => {
      val col = line.split("\t")
      (col(2), col(0), col(14).toLong)
    }).groupBy(x => (x._1)).mapValues(_.maxBy(_._3)).values
    awardWinningTitlesRDD.foreach(println)

    val jsonAwardWinningTitlesRDD = jsonRDD.map(_.replaceAll("[{}]", ""))
      .map(line => {
        val col = line.split("\",\"")
        ((col(3).replaceAll("\"", "")).split(":")(1), (col(1).replaceAll("\"", "")).split(":")(1), (col(15).replaceAll("\"", "")).split(":")(1).toLong)
      }).groupBy(x => (x._1)).mapValues(_.maxBy(_._3)).values
    jsonAwardWinningTitlesRDD.foreach(println)


    //  DATAFRAME
    val csvDataFrame = sparkSession.read.options(Map("delimiter" -> "\t", "header" -> "true", "inferSchema" -> "true"))
      .csv(csvFileInputPath)

    val jsonDataFrame = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .json(jsonFileInputPath)

    val queryOneCsvDF = csvDataFrame.where("language like '%English%'")
      .orderBy("year")
      .groupBy("year")
      .agg(count("year").as("no of language"))
      .select("year", "no of language")
    queryOneCsvDF.show()

    val queryOneJsonDF = jsonDataFrame.where("language like '%English%'").
      orderBy("year").groupBy("year")
      .agg(count("year").as("no of language"))
      .select("year", "no of language")
    queryOneJsonDF.show()

    val queryTwoCsvDF = csvDataFrame.orderBy(sortCol = "year")
      .groupBy("year")
      .agg(max(struct("title", "votes")).as("all_column"))
      .select("year", "all_column.*")
    queryTwoCsvDF.show()

    val queryTwoJsonDF = jsonDataFrame.orderBy(sortCol = "year")
      .groupBy("year")
      .agg(max(struct("title", "votes")).as("all_column"))
      .select("year", "all_column.*")
    queryTwoJsonDF.show()

    import sparkSession.implicits._
    val queryThirdCsvDF = csvDataFrame
      .filter($"year" >= startYear && $"year" <= endYear).groupBy("director")
      .agg(count("title").as("noOfTitle"), sum("votes").as("sumOfVotes"))
      .withColumn("highest_votes", col("sumOfVotes").divide(col("noOfTitle")))
      .select("director", "highest_votes")
    queryThirdCsvDF.show()

    val queryThirdJsonDF = jsonDataFrame
      .filter($"year" >= startYear && $"year" <= endYear).groupBy("director")
      .agg(count("title").as("noOfTitle"), sum("votes").as("sumOfVotes"))
      .withColumn("highest_votes", col("sumOfVotes").divide(col("noOfTitle")))
      .select("director", "highest_votes")
    queryThirdJsonDF.show()

    //DATASET
    val schema = Encoders.product[MoviesCSV].schema
    val csvDataSet = sparkSession.read.options(Map("delimiter" -> "\t", "header" -> "true"))
      .schema(schema)
      .csv(csvFileInputPath).as[MoviesCSV]

    val jsonDataSet = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .json(jsonFileInputPath).as[MoviesJSON]

    val queryOneCsvDS = csvDataSet.where("language like '%English%'")
      .orderBy("year")
      .groupBy("year")
      .agg(count("year").as("noOfLanguage"))
      .select("year", "noOfLanguage").as[NoOfEnglishPerYearCSV]

    val queryOneJsonDS = jsonDataSet.where("language like '%English%'")
      .orderBy("year")
      .groupBy("year")
      .agg(count("year").as("noOfLanguage"))
      .select("year", "noOfLanguage").as[NoOfEnglishPerYearJSON]

    val queryTwoCsvDS = csvDataSet.orderBy(sortCol = "year")
      .groupBy("year")
      .agg(max(struct("title", "votes")).as("all_column"))
      .select("year", "all_column.*").as[AwardWinningTitlesCSV]

    val queryTwoJsonDS = jsonDataSet.orderBy(sortCol = "year")
      .groupBy("year")
      .agg(max(struct("title", "votes")).as("all_column"))
      .select("year", "all_column.*").as[AwardWinningTitlesJSON]

    val queryThirdCsvDS = csvDataSet
      .filter($"year" >= startYear && $"year" <= endYear).groupBy("director")
      .agg(count("title").as("noOfTitle"), sum("votes").as("sumOfVotes"))
      .withColumn("highest_votes", col("sumOfVotes").divide(col("noOfTitle")))
      .select("director", "highest_votes").as[DirectorListCSV]

    val queryThirdJsonDS = jsonDataSet
      .filter($"year" >= startYear && $"year" <= endYear).groupBy("director")
      .agg(count("title").as("noOfTitle"), sum("votes").as("sumOfVotes"))
      .withColumn("highest_votes", col("sumOfVotes").divide(col("noOfTitle")))
      .select("director", "highest_votes").as[DirectorListJSON]

    // DATAFRAME writing to source

    queryOneCsvDF.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(fileOutputPath + "queryOneCsvDF.csv")

    queryOneJsonDF.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryOneJsonDF.json")

    queryOneJsonDF.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryOneParquetDF.parquet")

    queryTwoCsvDF.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(fileOutputPath + "queryTwoCsvDF.csv")

    queryTwoJsonDF.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryTwoJsonDF.json")

    queryTwoJsonDF.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryTwoParquetDF.parquet")

    queryThirdCsvDF.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(fileOutputPath + "queryThirdCsvDF.csv")

    queryThirdJsonDF.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryThirdJsonDF.json")

    queryThirdJsonDF.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryThirdParquetDF.parquet")


    // DATASET writing to source

    queryOneCsvDS.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryOneCsvDS.csv")

    queryOneJsonDS.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryOneJsonDS.json")

    queryOneJsonDS.coalesce(1).write.mode("overwrite")
      .option("header", "true").parquet(fileOutputPath + "queryOneParquetDS.parquet")

    queryTwoCsvDS.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryTwoCsvDS.csv")

    queryTwoJsonDS.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryTwoJsonDS.json")

    queryTwoJsonDS.coalesce(1).write.mode("overwrite")
      .option("header", "true").parquet(fileOutputPath + "queryTwoParquetDS.parquet")

    queryThirdCsvDS.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryThirdCsvDS.csv")

    queryThirdJsonDS.coalesce(1).write.mode("overwrite")
      .option("header", "true").json(fileOutputPath + "queryThirdJsonDS.json")

    queryThirdJsonDS.coalesce(1).write.mode("overwrite")
      .option("header", "true").parquet(fileOutputPath + "queryThirdParquetDS.parquet")

  }
}
