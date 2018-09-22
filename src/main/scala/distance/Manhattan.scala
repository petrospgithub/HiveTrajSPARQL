package distance

object Manhattan extends Distance {
  override def get(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val xcoord = Math.abs (lon1 - lon2)
    val ycoord = Math.abs (lat1- lat2)
    xcoord+ycoord
  }
}
