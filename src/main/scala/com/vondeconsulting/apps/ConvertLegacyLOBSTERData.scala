package com.vondeconsulting.apps

import com.vondeconsulting.lobster.LobsterData
import com.vondeconsulting.utils.Logging
import org.apache.spark.sql.SparkSession

object ConvertLegacyLOBSTERData extends App with Logging{
  val inputDir = args(0)
  val outputDir = args(1)
  val level = args(2).toInt
  val tempDir = if (args.length>3) args(3) else "/tmp"
  val spark = SparkSession.builder()
    .appName("Convert Legacy Lobster data")
    .master("local[2]")
    .getOrCreate()

  val lobsterData = new LobsterData(spark, outputDir)
  lobsterData.convertLegacyData(inputDir, level, tempDir)
}
