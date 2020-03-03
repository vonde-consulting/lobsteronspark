package com.vondeconsulting.utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.mutable

object Spark {

  /**
   * Convert LOBSTER data in CSV format to parquet files
   *
   * @param lobsterDataDir Lobster data csv directory
   * @param level          level of book
   * @param spark          spark session
   */
  def lobsterCsvToSparkDataFrame(lobsterDataDir: String, level: Int)(implicit spark: SparkSession): DataFrame = {
    import java.io.File
    val fileList = (new File(lobsterDataDir)).listFiles().map(s => s.getName.split("/").last)
    val organizedFiles = fileList.filter(_.contains("orderbook")).map(s => {
      val parts = s.split("_")
      (parts(0), parts(1), lobsterDataDir + "/" + s, lobsterDataDir + "/" + s.replace("orderbook", "message"))
    })
    val bookSchema = StructType(
      (1 to level).flatMap(x => List(
        StructField("ask_" + x, LongType, false),
        StructField("ask_size_" + x, LongType, false),
        StructField("bid_" + x, LongType, false),
        StructField("bid_size_" + x, LongType, false)
      )).toArray
    )

    val messageSchema = StructType(Array(
      StructField("time", DoubleType, false),
      StructField("event_id", IntegerType, false),
      StructField("order_id", LongType, false),
      StructField("quantity", LongType, false),
      StructField("price", LongType, false),
      StructField("side_id", IntegerType, false),
      StructField("mpid", StringType, false)
    ))

    val resultDf = organizedFiles.groupBy(_._2).map(x => x._2.map(s => {
      val bookDf = spark.read.schema(bookSchema).csv(s._3).addRowNumberColumn()

      val messageDf = spark.read.schema(messageSchema).csv(s._4)
        .withColumn("ticker", lit(s._1))
        .withColumn("date", lit(x._1))
        .addRowNumberColumn()
      bookDf.join(messageDf, "row_number")
    })
    ).reduce(_ union _).reduce(_ union _)

    resultDf.withColumn("sequence_number", monotonically_increasing_id())
      .drop("row_number")

  }

  /**
   * Resample the data base on time interval
   *
   * @param dfWithTimeColumn A DataFrame with a "time" column
   * @param intervalSeconds  Interval seconds
   * @return DataFrame. Time is the interval time, while tick_time is the tick time
   */
  def intervalResampleWithGaps(dfWithTimeColumn: DataFrame, intervalSeconds: Int, partitionColumnNames: Seq[String]): DataFrame = {
    val intervalNanoseconds = intervalSeconds * 1000000000L

    /**
     * Logic:
     * 1) Repartition the dataframe by ticker
     * 2) Sort the records by reversed timestamp, because the dropDuplicate in Spark always takes the first record and we like to take the last record in the bucket
     * 3) Divide the time plus 1 nanosecond by interval, then get the ceiling as the interval timestamp
     * 4) Drop the duplicates in terms of  ticker and interval timestamp
     * 5) Rename the interval timestamp to time
     * 6) Repartition by ticker and sort by the time
     */
    val partitionColumns = partitionColumnNames.map(col)
    dfWithTimeColumn.repartition(partitionColumns: _*)
      .sortWithinPartitions(desc("time"))
      .withColumn("interval_time", ceil((col("time") + 1) / intervalNanoseconds) * intervalSeconds)
      .dropDuplicates("interval_time" +: partitionColumnNames)
      .withColumnRenamed("time", "tick_time")
      .withColumnRenamed("interval_time", "time")
      .repartition(partitionColumns: _*)
      .sortWithinPartitions("time")
  }

  def intervalResampleWithoutGaps(dfWithTimeColumn: DataFrame, intervalSeconds: Int, partitionColumnNames: Seq[String],
                                  startTimeInSeconds: Int = 34200, endTimeInSeconds: Int = 57600)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val partitionColumns = partitionColumnNames.map(col)
    val selectedColumns = col("time") +: col("row_number") +: partitionColumns
    val dfWithTempSeq = dfWithTimeColumn.addRowNumberColumn()

    val numberBins = (endTimeInSeconds - startTimeInSeconds) / intervalSeconds
    val binEndTime = (0L to numberBins).map(r => (r * intervalSeconds + startTimeInSeconds, r))
    val binEndTimeVal = spark.sparkContext.broadcast(binEndTime)
    val matchedSeq = dfWithTempSeq.select(selectedColumns: _*)
      .repartition(partitionColumns: _*)
      .sortWithinPartitions(col("time"))
      .mapPartitions(it =>
        if (it.hasNext) {
          val localBinEndTime = binEndTimeVal.value.map(x => (x._1 * 1000000000L, x._2))
          val dfSeq = it.map(r => (r.getLong(0), r.getLong(1)))
          val myMatchedSeq = iteratorAsOfMatch(localBinEndTime.toIterator, dfSeq)
          myMatchedSeq.toIterator
        } else
          Array[(Long, Long)]().toIterator
      ).withColumnRenamed("_1", "row_seq_num")
      .withColumnRenamed("_2", "row_number")
    val binEndTimeDf0 = binEndTime.toDF("interval_time", "row_seq_num")
    val partitionDf = dfWithTimeColumn.select(partitionColumns: _*).distinct()
    val binEndTimeDf = partitionDf.crossJoin(binEndTimeDf0)
    val dfWithMatchedRowNumber = matchedSeq.join(dfWithTempSeq, Seq("row_number"), "left_outer")
    binEndTimeDf.join(dfWithMatchedRowNumber, "row_seq_num" +: partitionColumnNames, "left_outer")
      .withColumnRenamed("time", "tick_time")
      .withColumnRenamed("interval_time", "time")
      .drop("row_seq_num", "row_number")
      .repartition(partitionColumns: _*)
      .sortWithinPartitions("time")
  }

  def iteratorAsOfMatch[T, K, U](leftIterator: Iterator[(T, K)], rightIterator: Iterator[(T, U)])(implicit order: T => Ordered[T]): Array[(K, U)] = {
    val result = mutable.ArrayBuffer[(K, U)]()
    var leftElem = leftIterator.next()
    var rightElem = rightIterator.next()
    var prevRightKey: U = rightElem._2
    // Move the first element with a time greater than the first right element time
    while (leftElem._1 < rightElem._1 && leftIterator.hasNext)
      leftElem = leftIterator.next

    while (leftIterator.hasNext && rightIterator.hasNext) {
      while (leftElem._1 > rightElem._1 && rightIterator.hasNext) {
        prevRightKey = rightElem._2
        rightElem = rightIterator.next
      }
      result.append((leftElem._2, prevRightKey))
      leftElem = leftIterator.next
    }

    // If right end earlier
    if (leftIterator.hasNext && !rightIterator.hasNext) {
      if (leftElem._1 > rightElem._1)
        prevRightKey = rightElem._2
      while (leftIterator.hasNext) {
        result.append((leftElem._2, prevRightKey))
        leftElem = leftIterator.next
      }
    }
    // last element of left iterator
    if (rightIterator.hasNext) {
      while (leftElem._1 > rightElem._1 && rightIterator.hasNext) {
        prevRightKey = rightElem._2
        rightElem = rightIterator.next
      }
    }

    result.append((leftElem._2, prevRightKey))

    result.toArray
  }

  /**
   * Add a row number column to a spark dataframe
   *
   * @param df    Dataframe
   * @param spark Spark session
   */
  implicit class DataFrameExtension(val df: DataFrame)(implicit spark: SparkSession) {
    def addRowNumberColumn(): DataFrame = spark.sqlContext.createDataFrame(
      // Add Column index
      df.rdd.zipWithIndex.map { case (row, row_number) => Row.fromSeq(row.toSeq :+ row_number) },
      // Create schema
      StructType(df.schema.fields :+ StructField("row_number", LongType, false))
    )

    def intervalResampleWithoutGaps(intervalSeconds: Int, partitionColumnNames: Seq[String],
                                    startTimeInSeconds: Int = 34200, endTimeInSeconds: Int = 57600): DataFrame =
      Spark.intervalResampleWithoutGaps(df, intervalSeconds, partitionColumnNames, startTimeInSeconds, endTimeInSeconds)


    def intervalResampleWithGaps(intervalSeconds: Int, partitionColumnNames: Seq[String]): DataFrame =
      Spark.intervalResampleWithGaps(df, intervalSeconds, partitionColumnNames)
  }
}
