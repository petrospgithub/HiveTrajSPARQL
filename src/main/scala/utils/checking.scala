package utils

import java.util

import org.apache.hadoop.hive.serde2.objectinspector.{SettableStructObjectInspector, StructField}

import scala.collection.immutable.HashSet

object checking {

  val hashSet_box = HashSet("id","minx","maxx","miny","maxy","mint","maxt")
  val hashSet_box_noID = HashSet("minx","maxx","miny","maxy","mint","maxt")

  val hashSet_point = HashSet("longitude", "latitude", "timestamp")

  def mbb (box: SettableStructObjectInspector):Boolean={
    var check = true

    val temp=box.getAllStructFieldRefs

    val it: util.Iterator[StructField] = temp.iterator.asInstanceOf[util.Iterator[StructField]]


    if (temp.size()==6) {
      while ( it.hasNext)
        if (!hashSet_box_noID.contains(it.next.getFieldName)) check = false
    } else if (temp.size()==7) {
      while ( it.hasNext)
        if (!hashSet_box.contains(it.next.getFieldName)) check = false
    } else {
      check=false
    }

    check
  }

  def point (point: SettableStructObjectInspector):Boolean={
    var check = true

    val it: util.Iterator[StructField] = point.getAllStructFieldRefs.iterator.asInstanceOf[util.Iterator[StructField]]


    while ( it.hasNext)
      if (!hashSet_point.contains(it.next.getFieldName)) check = false

    check
  }

}
