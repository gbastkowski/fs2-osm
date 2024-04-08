package fs2.osm

case class Coordinate(longitude: Double, latitude: Double) {

  override def hashCode = (longitude * 1000000).toInt + (latitude * 1000000).toInt

  /** Precision of 6 decimal digits is enough */
  override def equals(other: Any) =
    other match {
      case that: Coordinate =>
        (this.longitude * 1000000).toInt == (that.longitude * 1000000).toLong &&
        (this.latitude  * 1000000).toInt == (that.latitude  * 1000000).toLong
      case _ => false
    }
}
