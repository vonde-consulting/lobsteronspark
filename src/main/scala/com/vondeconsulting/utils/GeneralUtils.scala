package com.vondeconsulting.utils

import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source

object GeneralUtils {

  def listFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def zipCSVFiles(leftFile:String, rightFile:String): Array[String] = {
    val leftSource = Source.fromFile(leftFile)
    val leftLines = leftSource.getLines.toArray
    val rightSource = Source.fromFile(rightFile)
    val rightLines = rightSource.getLines.toArray
    (leftLines zip rightLines).map(s => s._1 + "," + s._2)
  }

  def zipCSVFiles(leftFile:String, rightFile:String, outFile:String):Unit = {
    val lines = zipCSVFiles(leftFile, rightFile)
    val of = new BufferedWriter(new FileWriter(outFile))
    lines.foreach(s=> of.write(s + "\n") )
    of.close()
  }

  def convertToIsoDateString(lobsterDateString: String): String = {
    val isoFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val lobsterFormatter = new java.text.SimpleDateFormat("yyyyMMdd")
    isoFormatter.format(lobsterFormatter.parse(lobsterDateString))
  }
}
