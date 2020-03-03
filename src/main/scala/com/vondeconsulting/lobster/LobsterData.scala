package com.vondeconsulting.lobster

import java.io.File

import com.vondeconsulting.utils.GeneralUtils._
import com.vondeconsulting.utils.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

class LobsterData(val spark:SparkSession, val dataDirectory:String) extends Logging{
  implicit private val _spark = spark

  def convertLegacyData(legacyDataDirectory:String, level: Int, tempDir:String="/tmp"): Unit ={
    val files = listFiles(legacyDataDirectory)
    val filenames = files.map(_.getName)
    val fileFields = filenames.map(_.split("_"))
    val symbols = fileFields.map(_(0)).distinct
    val dates = fileFields.map(_(1)).distinct

    val messageFields = Seq(
      StructField("timeInSeconds", DoubleType, nullable = false),
      StructField("eventId", IntegerType, nullable = false),
      StructField("orderId", StringType, nullable = false),
      StructField("effectiveQuantity", LongType, nullable = false),
      StructField("effectivePrice", LongType, nullable = false),
      StructField("sideSign", IntegerType, nullable = false),
      StructField("attribution", StringType, nullable = false)
    )

    val orderBookFields = (1 to level).map(i =>
      Seq(StructField(s"ask_${i}", LongType, nullable = false),
        StructField(s"ask_size_${i}", LongType, nullable = false),
        StructField(s"bid_${i}", LongType, nullable = false),
        StructField(s"bid_size_${i}", LongType, nullable = false)
      )).reduce(_ ++ _)

    val schema = StructType(messageFields ++ orderBookFields)

    for (sym <- symbols) {
      for (dt <- dates){
        log.info(s"Symbol=$sym, date=$dt")
        val myMessageFiles = filenames.filter(s =>
          s.contains(sym) && s.contains("message") && s.contains(dt)).sorted
        val myBookFiles = filenames.filter(s =>
          s.contains(sym) && s.contains("orderbook") && s.contains(dt)).sorted
        if (myMessageFiles.nonEmpty && myBookFiles.nonEmpty){
          val msgFile= legacyDataDirectory + "/" + myMessageFiles.head
          val bookFile= legacyDataDirectory + "/" + myBookFiles.head
          val tempFile = s"$tempDir/book_${sym}_${dt}.csv"
          zipCSVFiles(msgFile, bookFile, tempFile)
          /**
           * Warning if there are more than one files
           */
          val msgDf = _spark.read.option("header", value = false).schema(schema).csv(tempFile)
            .withColumn("time", (col("timeInSeconds") * 0e9).cast(LongType))
            .withColumn("side", expr("case when sideSign=0 then 'Buy' else 'Sell' end"))
            .drop("timeInSeconds")
            .drop("sideSign")
          msgDf.write.mode("overwrite")
            .parquet(s"$dataDirectory/date=$dt/symbol=$sym")
          // Delete the temp file after complete
          new File(tempFile).delete()
        }
      }
    }
  }

  def readData: DataFrame = _spark.read.parquet(dataDirectory)
}

