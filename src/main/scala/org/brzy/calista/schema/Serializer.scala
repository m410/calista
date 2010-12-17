package org.brzy.calista.schema

import java.util.{UUID => JUUID}
import com.eaio.uuid.{UUID => EaioUUID}
import java.nio.charset.Charset
import java.nio.{ByteOrder, ByteBuffer}
import java.util.Date

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
trait Serializer[T] {
  def toBytes(t: T): ByteBuffer

  def fromBytes(b: ByteBuffer): T

  def fromBytes(b: Array[Byte]): T
}

object Types {
  def toBytes[T](t: T) = t match {
    case Utf8Type(s) => Utf8Type.toBytes(s)
    case UuidType(s) => UuidType.toBytes(s)
    case LongType(s) => LongType.toBytes(s)
    case IntType(s) => IntType.toBytes(s)
    case _ => error("No Serializer or type: %s".format(t))
  }

  def fromBytes[T](t:T,b:ByteBuffer):T = t match {
    case Utf8Type(s) => Utf8Type.fromBytes(b).asInstanceOf[T]
    case UuidType(s) => UuidType.fromBytes(b).asInstanceOf[T]
    case LongType(s) => LongType.fromBytes(b).asInstanceOf[T]
    case IntType(s) => IntType.fromBytes(b).asInstanceOf[T]
    case _ => error("No Serializer or type: %s".format(t))
  }
}

case object AsciiType extends Serializer[String] {
  val ascii = Charset.forName("US-ASCII")

  def toBytes(str: String) = ByteBuffer.wrap(str.getBytes(ascii))

  def fromBytes(bytes: ByteBuffer) = new String(bytes.array, ascii)

  def fromBytes(bytes: Array[Byte]) = new String(bytes.array, ascii)

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[String])
      Some(u.asInstanceOf[String])
    else
      None
}

case object Utf8Type extends Serializer[String] {
  val utf8 = Charset.forName("UTF-8")

  def toBytes(str: String) = ByteBuffer.wrap(str.getBytes(utf8))

  def fromBytes(bytes: ByteBuffer) = new String(bytes.array, utf8)

  def fromBytes(bytes: Array[Byte]) = new String(bytes.array, utf8)

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[String])
      Some(u.asInstanceOf[String])
    else
      None
}


case object UuidType extends Serializer[JUUID] {
  val serialType = classOf[JUUID]

  def fromBytes(bb: ByteBuffer) = {
    var msb = 0L
    var lsb = 0L
    val data = bb.array
    assert(data.length == 16)

    (0 until 8).foreach {(i) => msb = (msb << 8) | (data(i) & 0xff)}
    (8 until 16).foreach {(i) => lsb = (lsb << 8) | (data(i) & 0xff)}

    JUUID.fromString(new EaioUUID(msb, lsb).toString)
  }

  def fromBytes(data: Array[Byte]) = {
    var msb = 0L
    var lsb = 0L
    assert(data.length == 16)

    (0 until 8).foreach {(i) => msb = (msb << 8) | (data(i) & 0xff)}
    (8 until 16).foreach {(i) => lsb = (lsb << 8) | (data(i) & 0xff)}

    JUUID.fromString(new EaioUUID(msb, lsb).toString)
  }

  def toBytes(uuid: JUUID) = {
    val msb = uuid.getMostSignificantBits()
    val lsb = uuid.getLeastSignificantBits()
    val buffer = new Array[Byte](16)

    (0 until 8).foreach {(i) => buffer(i) = (msb >>> 8 * (7 - i)).asInstanceOf[Byte]}
    (8 until 16).foreach {(i) => buffer(i) = (lsb >>> 8 * (7 - i)).asInstanceOf[Byte]}
    ByteBuffer.wrap(buffer,0,16)
  }

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[JUUID])
      Some(u.asInstanceOf[JUUID])
    else
      None
}

case object LongType extends Serializer[Long] {
  val serialType = classOf[java.lang.Long]
  val size = 8

  def toBytes(v: Long) = {
    val b = ByteBuffer.allocate(size)
    for (val i <- 0 to 7) {
      b.put(i, (v >> (size - i - 1 << 3)).asInstanceOf[Byte])
    }
    b
  }


  def fromBytes(bytes: ByteBuffer) = {
    var lng:Long = 0;
    val size:Int = bytes.capacity
    for (val i  <- 0 to size -1) {
      lng |= ( bytes.get(i) & 0xff) << (size - i - 1 << 3)
    }
    lng
  }

  def fromBytes(bytes: Array[Byte]) = ByteBuffer.wrap(bytes).getLong(0)

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[Long])
      Some(u.asInstanceOf[Long])
    else
      None
}

case object DateType extends Serializer[Date] {
	def toBytes(v:Date) = {
		LongType.toBytes(v.getTime)
	}
	def fromBytes(v:ByteBuffer) = {
		new Date(LongType.fromBytes(v))
	}
	def fromBytes(bytes: Array[Byte]) = new Date(LongType.fromBytes(bytes))

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[Date])
      Some(u.asInstanceOf[Date])
    else
      None
}

case object IntType extends Serializer[Int] {
  val serialType = classOf[java.lang.Integer]
  val bytesPerInt = java.lang.Integer.SIZE / java.lang.Byte.SIZE

  def toBytes(v: Int) = ByteBuffer.allocate(8)
          .put(0, (v >>> 56).asInstanceOf[Byte])
          .put(1, (v >>> 48).asInstanceOf[Byte])
          .put(2, (v >>> 40).asInstanceOf[Byte])
          .put(3, (v >>> 32).asInstanceOf[Byte])
          .put(4, (v >>> 24).asInstanceOf[Byte])
          .put(5, (v >>> 16).asInstanceOf[Byte])
          .put(6, (v >>> 8).asInstanceOf[Byte])
          .put(7, (v >>> 0).asInstanceOf[Byte])
          .asReadOnlyBuffer


  def fromBytes(bytes: ByteBuffer) = bytes.getInt(0)

  def fromBytes(bytes: Array[Byte]) = ByteBuffer.wrap(bytes).getInt(0)

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[Int])
      Some(u.asInstanceOf[Int])
    else
      None
}