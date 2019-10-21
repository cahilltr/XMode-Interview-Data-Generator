package io.xmode.cahill.interview.data.generation

import java.io.InputStream
import java.nio.file.Paths
import java.util.Date

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.locationtech.spatial4j.context.SpatialContext
import org.locationtech.spatial4j.distance.DistanceUtils
import org.locationtech.spatial4j.shape.Point
import org.locationtech.spatial4j.shape.impl.PointImpl
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.Random

object GenerateParquet {
  private val logger = LoggerFactory.getLogger(classOf[Nothing])

  private val spCtx: SpatialContext = SpatialContext.GEO
  private val random = new Random()
  val schema: Schema = SchemaBuilder
    .record("Location")
    .fields()
    .requiredString("advertiser_id")
    .requiredLong("location_at")
    .requiredDouble("latitude")
    .requiredDouble("longitude")
    .endRecord()

  val advertiserIDs = Seq("CompanyA", "CompanyB", "CompanyC", "CompanyD", "CompanyE")

  private val NUM_LOCATIONS:String = "numLocations"
  private val PERCENT_LOCATIONS_NEAR:String = "percentLocationsNear"
  private val STARTDATE:String = "startDateMillis"
  private val ENDDATE:String = "endDateMillis"
  private val OUTPUT_FILE:String = "outputFile"
  private val NUM_OUT_OF_DATE_LOCATIONS:String = "numOutOfDateLocations"

  /**
    * To command line arguments should be in format "name=value" "name2=value2"
    * i.e. outputFile=/Users/cahillt/Desktop/myParquet.parquet numLocations=10000 percentLocationsNear=.5
    */
  def main (args: Array[String]): Unit = {
    val optMap = args.map(s => {
      val split = s.split("=")
      (split(0), split(1))
    }).toMap

    val numLocations = if (optMap.contains(NUM_LOCATIONS)) optMap.getOrElse(NUM_LOCATIONS, "1000").toInt else 1000
    val numOutOfDateLocs = if (optMap.contains(NUM_OUT_OF_DATE_LOCATIONS)) optMap.getOrElse(NUM_OUT_OF_DATE_LOCATIONS, "100").toInt else 100
    val percentLocationsNear = if (optMap.contains(PERCENT_LOCATIONS_NEAR)) optMap.getOrElse(PERCENT_LOCATIONS_NEAR, "0.75").toDouble else 0.75
    val startDate = if (optMap.contains(STARTDATE)) new Date(optMap.getOrElse(STARTDATE, "1526939409").toLong) else new Date(1526939409)
    val endDate = if (optMap.contains(ENDDATE)) new Date(optMap.getOrElse(ENDDATE, "1530741009").toLong) else new Date(1530741009)
    val outputFile = if (optMap.contains(OUTPUT_FILE)) optMap.getOrElse(OUTPUT_FILE, "/Users/cahillt/Desktop/myParquet.parquet") else "/Users/cahillt/Desktop/myParquet.parquet"

    val poi = csvToLocations(Thread.currentThread().getContextClassLoader.getClass.getResourceAsStream("/top100.csv"))
    val locations = generateLocations(numLocations,percentLocationsNear, poi, startDate, endDate)
    locations.foreach(System.out.println)
    val outOfDate = generateOutOfDateLocations(numOutOfDateLocs, startDate, endDate)
    outOfDate.foreach(System.out.println)
    if (!outputFile.isEmpty)
      writeParquet(locations ++ outOfDate, outputFile)
  }

  //https://stackoverflow.com/questions/45842226/how-to-make-a-parquet-file-from-scala-case-class-using-avroparquetwriter
  def writeParquet(data: Iterable[Location], outputPathString: String): Unit = {
    val outputPath = Paths.get(outputPathString)

    val writer = AvroParquetWriter.builder[Location](OutputFileConverstion.nioPathToOutputFile(outputPath))
      .withRowGroupSize(256 * 1024 * 1024)
      .withPageSize(128 * 1024)
      .withSchema(schema)
      .withConf(new Configuration())
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withDataModel(ReflectData.get)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withValidation(false)
      .withDictionaryEncoding(false)
      .build()

    data.foreach(writer.write)
    writer.close()
  }

  def readParquet(inputStringPath:String): Unit = {
    val reader = AvroParquetReader.builder[GenericData.Record](InputFileConversion.nioPathToInputFile(Paths.get(inputStringPath)))
      .withConf(new Configuration())
      .build()

    var location:GenericData.Record = null
    do {
      location = reader.read()
      System.out.println(location)
    } while (location != null)

    reader.close()
  }

  //TODO scalaify this to use a function that is passed a function
  def generateLocations(number:Long, percentLocationsNear:Double, pointsNear:Seq[Location], startDate:Date, endDate:Date): Seq[Location] = {
    val numPointsNear = Math.ceil(number * percentLocationsNear).toInt
    val numPointsAway = Math.floor(number * (1 - percentLocationsNear)).toInt
    val pointsNearTotal = pointsNear.length
    val advertiserLength = advertiserIDs.length
    val start = startDate.getTime
    val end   = endDate.getTime

    //X is Long, Y is Lat
    val fiftyMetersInDegrees = (30.0/1000) * DistanceUtils.KM_TO_DEG
    val sixtyMetersInDegrees = (60.0/1000) * DistanceUtils.KM_TO_DEG

    val locationsNear = (1 to numPointsNear).map(i =>{
      val nearLoc = pointsNear(i%pointsNearTotal)
      val pointNew = randomPointWithinCircle(nearLoc.getLatitude, nearLoc.getLongitude, fiftyMetersInDegrees)
      val randomDate = start + (random.nextDouble()*(end - start)).toLong
      new Location(advertiserIDs(i%advertiserLength), randomDate, pointNew.getX, pointNew.getY)
    })

    val locationsAway = (1 to numPointsAway).map(i => {
      val nearLoc = pointsNear(i%pointsNearTotal)
      val changeLat = i%2 == 0
      val randomDate = start + (random.nextDouble()*(end - start)).toLong
      val pointNew = new PointImpl(validateLongitude(if (changeLat) nearLoc.getLongitude else nearLoc.getLongitude + sixtyMetersInDegrees),
        validateLatitude(if (changeLat) nearLoc.getLatitude + sixtyMetersInDegrees else nearLoc.getLatitude), spCtx)
      new Location(advertiserIDs(i%advertiserLength), randomDate, pointNew.getX, pointNew.getY)
    })

    locationsAway ++ locationsNear
  }

  //https://programming.guide/random-point-within-circle.html
  def randomPointWithinCircle(lat:Double, longitude:Double, radius:Double): Point = {
    val a = random.nextDouble() * 2 * Math.PI
    val r = radius * Math.sqrt(random.nextDouble())
    val x = r * Math.cos(a)
    val y = r * Math.sin(a)

    new PointImpl(validateLongitude(x + longitude), validateLatitude(y + lat), spCtx)
  }

  def generateOutOfDateLocations(numOutSideOfDates:Int, startDate:Date, endDate:Date): Seq[Location] = {
    (1 to numOutSideOfDates).map(i => {
      if (i%2 == 0) {
        new Location("fake", new Date(endDate.getTime + System.currentTimeMillis()).getTime, 0.0, 0.0)
      } else {
        new Location("fake", 1L, 0.0, 0.0)
      }
    })
  }

  def validateLatitude(lat:Double): Double = if (lat > 90) ((lat - 90) * -1) else if (lat < -90) ((lat + 90) * -1) else lat
  def validateLongitude(longitude:Double): Double = if (longitude > 180) ((longitude - 180) * -1) else if (longitude < -180) ((longitude + 180) * -1) else longitude

  def csvToLocations(csvFilePath: InputStream): Seq[Location] = {
    val lineIter = Source.fromInputStream(csvFilePath).getLines()
    val locs = lineIter.drop(1).map(line => {
      val cols = line.split(",").map(_.trim)
      new Location(cols(0), cols(3).toLong, cols(2).toDouble, cols(1).toDouble)
    }).toSeq
    locs
  }
}
