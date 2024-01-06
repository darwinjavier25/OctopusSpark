package org.example.etl

import connections.dataLake.ReadCsv
import org.apache.spark.sql.SparkSession
import org.example.connections.formats.ReadJson
import org.example.etl.landing.SpaceXLandingApi
import org.example.etl.transformations.NormalizeData

import java.io.FileInputStream
import java.util.Properties

object runeable extends NormalizeData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .appName("SparkJoinDataFrames")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    //Landing layer get data json format without any transformation
    val missionsEndPoint = "missions"
    val missionsFileDest = "spaceXMissionsApi.json"
    val launchesEndPoint = "launches"
    val launchesFileDest = "spaceXLaunchesApi.json"
    val setApiDf = new SpaceXLandingApi
    setApiDf.landJson(spark, missionsEndPoint, missionsFileDest)
    setApiDf.landJson(spark, launchesEndPoint, launchesFileDest)

    /*
    //Landing layer to get data in csv from data sources
    val props = new Properties()
    val path = "src/main/resources/config.properties"
    props.load(new FileInputStream(path))
    val sourcePath = props.getProperty("sources")
    val destPath = props.getProperty("destiny")
    val sourceFile = "SpaceX-Missions.csv"
    val destFile = "SpaceXMissions.csv"

    val setDfCsv = new SpaceXLandingCsv
    setDfCsv.ingestDF(spark, sourcePath,destPath, sourceFile, destFile)

    sql python
     */

    val props = new Properties()
    val path = "src/main/resources/config.properties"
    props.load(new FileInputStream(path))
    val destinyPath = props.getProperty("destiny")
    val sourcePath = props.getProperty("destiny")
    val fileCsv = "SpaceXMissions.csv"
    val fileJson = "spaceXMissionsApi.json"

    val setMissionsApiDf = new ReadJson
    val missionsApiDf = setMissionsApiDf.setDfJsonSource(spark, sourcePath, fileJson)

    val setDF = new ReadCsv
    val missionSourceDf = setDF.setDF(spark, destinyPath, fileCsv)
    val workDF = new NormalizeData
    val missionsSourceRemoveBlank = workDF.removeBlancks(missionSourceDf)
    val missionsSourceHeaderDf = workDF.transformColNames(missionsSourceRemoveBlank)
    val missionsSourceDateOkDf = workDF.transformDates(missionsSourceHeaderDf)
    val missionsSourceLowerDf = workDF.transformLower(missionsSourceDateOkDf)
    val missionsApiRemoveBlank = workDF.removeBlancks(missionsApiDf)
    val missionsApiHeaderDf = workDF.transformColNames(missionsApiRemoveBlank)
    val missionsApiDateOkDf = workDF.transformDates(missionsApiHeaderDf)
    val missionsApiLowerDf = workDF.transformLower(missionsApiDateOkDf)
    missionsSourceDateOkDf.printSchema()
    missionsSourceLowerDf.show()
    missionsApiLowerDf.printSchema()
    missionsApiLowerDf.show()
  }
}