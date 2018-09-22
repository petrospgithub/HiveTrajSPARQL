package distance

trait Distance {
  def get(lat1:Double, lon1:Double, lat2:Double, lon2:Double):Double
}
