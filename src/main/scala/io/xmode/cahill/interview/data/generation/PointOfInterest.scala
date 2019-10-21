package io.xmode.cahill.interview.data.generation

class PointOfInterest(name:String, longitudeDegrees:Double, latitudeDegrees:Double, radius:Double) {

  def getName:String = name
  def getLongitude:Double = longitudeDegrees
  def getLatitude:Double = latitudeDegrees
  def getRadius:Double = radius
}
