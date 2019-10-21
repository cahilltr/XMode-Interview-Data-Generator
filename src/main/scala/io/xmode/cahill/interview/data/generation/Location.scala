package io.xmode.cahill.interview.data.generation

import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData

class Location (advertiser_id:String, location_at:Long, longitude:Double, latitude:Double) {
  def getAdvertiser: String = advertiser_id
  def getLocationAt: Long = location_at
  def getLatitude: Double = latitude
  def getLongitude: Double = longitude

  //https://stackoverflow.com/questions/42078757/is-it-possible-to-read-and-write-parquet-using-java-without-a-dependency-on-hado
  def toGenericRecord: GenericRecord = {
    val record = new GenericData.Record(GenerateParquet.schema)
    record.put("advertiser_id", advertiser_id)
    record.put("location_at", location_at)
    record.put("latitude", latitude)
    record.put("longitude", longitude)
    record
  }

  def toLocation(record:GenericRecord): Location = {
    new Location(record.get("advertiser_id").toString, record.get("location_at").toString.toLong,
      record.get("latitude").toString.toDouble, record.get("longitude").toString.toDouble)
  }

  override def toString: String = advertiser_id + "," + location_at + "," + longitude + "," + latitude
}