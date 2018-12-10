package utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import di.thesis.indexing.types.{EnvelopeST, PointST}

object SerDerUtil {
  def trajectory_serialize(traj:Array[PointST]): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(traj)
    out.flush()
    bos.toByteArray.clone()
  }

  def trajectory_deserialize(traj:Array[Byte]): Array[PointST] = {
    val bis = new ByteArrayInputStream(traj)
    val in = new ObjectInputStream(bis)
    val retrievedObject = in.readObject.asInstanceOf[Array[PointST]]
    retrievedObject
  }

  def mbb_serialize(traj:EnvelopeST): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(traj)
    out.flush()
    bos.toByteArray.clone()
  }

  def mbb_deserialize(traj:Array[Byte]): EnvelopeST = {
    val bis = new ByteArrayInputStream(traj)
    val in = new ObjectInputStream(bis)
    val retrievedObject = in.readObject.asInstanceOf[EnvelopeST]
    retrievedObject
  }

}
