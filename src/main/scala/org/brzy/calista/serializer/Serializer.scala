/*
 * Copyright 2010 Michael Fortin <mike@brzy.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");  you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed 
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.brzy.calista.serializer

import java.nio.charset.Charset
import java.nio.{ByteOrder, ByteBuffer}
import java.util.{UUID, Date}

/**
 * Basic Serializer interface.
 *
 * @todo Need to add Object, Array, List, Map and Enum serializer.
 * @author Michael Fortin
 */
trait Serializer[T] {
  val typeManifest:Manifest[T]
  def serializedClass = typeManifest.erasure

  def toBytes(t: T): ByteBuffer

  def fromBytes(b: ByteBuffer): T

  def fromBytes(b: Array[Byte]): T
}

/**
 * Object factory for serializers.  Provides convenient access and referencing.
 * @todo this should be changed to an instance that can be updated with custom serializers.
 */
object Serializers {
  protected[serializer] val LongClass = classOf[Long]
  protected[serializer] val IntClass = classOf[Int]
  protected[serializer] val StringClass = classOf[String]
  protected[serializer] val UUIDClass = classOf[UUID]
  protected[serializer] val DateClass = classOf[Date]
  protected[serializer] val BooleanClass = classOf[Boolean]


  def toBytes[T](t: T) = t match {
  	case BooleanSerializer(s) => BooleanSerializer.toBytes(s)
    case UTF8Serializer(s) => UTF8Serializer.toBytes(s)
    case UUIDSerializer(s) => UUIDSerializer.toBytes(s)
    case LongSerializer(s) => LongSerializer.toBytes(s)
    case IntSerializer(s) => IntSerializer.toBytes(s)
    case DateSerializer(s) => DateSerializer.toBytes(s)
    case _ => error("No Serializer or type: %s".format(t))
  }

  def fromBytes[T](t:T,b:ByteBuffer):T = t match {
    case UTF8Serializer(s) => UTF8Serializer.fromBytes(b).asInstanceOf[T]
    case UUIDSerializer(s) => UUIDSerializer.fromBytes(b).asInstanceOf[T]
    case LongSerializer(s) => LongSerializer.fromBytes(b).asInstanceOf[T]
    case IntSerializer(s) => IntSerializer.fromBytes(b).asInstanceOf[T]
    case DateSerializer(s) => DateSerializer.fromBytes(b).asInstanceOf[T]
    case BooleanSerializer(s) => BooleanSerializer.fromBytes(b).asInstanceOf[T]
    case _ => error("No Serializer or type: %s".format(t))
  }

  def fromClassBytes[T](t:Class[T],b:Array[Byte]):T = t match {
    case StringClass => UTF8Serializer.fromBytes(b).asInstanceOf[T]
    case UUIDClass => UUIDSerializer.fromBytes(b).asInstanceOf[T]
    case LongClass => LongSerializer.fromBytes(b).asInstanceOf[T]
    case IntClass => IntSerializer.fromBytes(b).asInstanceOf[T]
    case DateClass => DateSerializer.fromBytes(b).asInstanceOf[T]
    case BooleanClass => BooleanSerializer.fromBytes(b).asInstanceOf[T]
    case _ => error("No Serializer or type: %s".format(t))
  }
}

/**
 * ASCII string serializer.
 */
case object ASCIISerializer extends Serializer[String] {
  val ascii = Charset.forName("US-ASCII")
  val typeManifest = manifest[String]

  def toBytes(str: String) = ByteBuffer.wrap(str.getBytes(ascii))

  def fromBytes(bytes: ByteBuffer) = new String(bytes.array, ascii)

  def fromBytes(bytes: Array[Byte]) = new String(bytes.array, ascii)

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[String])
      Some(u.asInstanceOf[String])
    else
      None
}

/**
 * UTF8 String serializer
 */
case object UTF8Serializer extends Serializer[String] {
  val utf8 = Charset.forName("UTF-8")
  val typeManifest = manifest[String]

  def toBytes(str: String) = ByteBuffer.wrap(str.getBytes(utf8))

  def fromBytes(bytes: ByteBuffer) = new String(bytes.array, utf8)

  def fromBytes(bytes: Array[Byte]) = new String(bytes.array, utf8)

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[String])
      Some(u.asInstanceOf[String])
    else
      None
}


/**
 * UUID String serializer.  UUID's get special treatment in cassandra.
 */
case object UUIDSerializer extends Serializer[UUID] {
  val serialType = classOf[UUID]
  val typeManifest = manifest[UUID]

  def fromBytes(bb: ByteBuffer) = new UUID(bb.getLong, bb.getLong)

  def fromBytes(data: Array[Byte]) = {
    val bb = ByteBuffer.wrap(data)
    new UUID(bb.getLong,bb.getLong)
  }
  
//  def fromBytes(data: Array[Byte]) = {
//    var msb = 0L
//    var lsb = 0L
//
//    assert(data.length == 16,"data length not 16 bytes: " + data.length)
//
//    (0 until 8).foreach {(i) => msb = (msb << 8) | (data(i) & 0xff)}
//    (8 until 16).foreach {(i) => lsb = (lsb << 8) | (data(i) & 0xff)}
//
//    new UUID(msb, lsb)
//  }

  def toBytes(uuid: UUID) = {
    val msb = uuid.getMostSignificantBits
    val lsb = uuid.getLeastSignificantBits
    val buffer = new Array[Byte](16)

    (0 until 8).foreach {(i) => buffer(i) = (msb >>> 8 * (7 - i)).asInstanceOf[Byte]}
    (8 until 16).foreach {(i) => buffer(i) = (lsb >>> 8 * (7 - i)).asInstanceOf[Byte]}
    ByteBuffer.wrap(buffer,0,16)
  }

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[UUID])
      Some(u.asInstanceOf[UUID])
    else
      None
}

/**
 * Long serializer
 */
case object LongSerializer extends Serializer[Long] {
  val serialType = classOf[java.lang.Long]
  val size = 8
  val typeManifest = manifest[Long]

  def toBytes(v: Long) = ByteBuffer.allocate(size).putLong(0, v)

  def fromBytes(bytes: ByteBuffer) = bytes.getLong

  def fromBytes(bytes: Array[Byte]) = ByteBuffer.wrap(bytes).getLong(0)

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[Long])
      Some(u.asInstanceOf[Long])
    else
      None
}

/**
 * Date Serializer
 */
case object DateSerializer extends Serializer[Date] {
  val typeManifest = manifest[Date]

	def toBytes(v:Date) = LongSerializer.toBytes(v.getTime)

	def fromBytes(v:ByteBuffer) = new Date(LongSerializer.fromBytes(v))
  
	def fromBytes(bytes: Array[Byte]) = new Date(LongSerializer.fromBytes(bytes))

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[Date])
      Some(u.asInstanceOf[Date])
    else
      None
}

/**
 * Integer serializer
 */
case object IntSerializer extends Serializer[Int] {
  val serialSerializer = classOf[java.lang.Integer]
  val bytesPerInt = java.lang.Integer.SIZE / java.lang.Byte.SIZE
  val typeManifest = manifest[Int]

//  def toBytes(v: Int) = ByteBuffer.allocate(8)
//          .put(0, (v >>> 56).asInstanceOf[Byte])
//          .put(1, (v >>> 48).asInstanceOf[Byte])
//          .put(2, (v >>> 40).asInstanceOf[Byte])
//          .put(3, (v >>> 32).asInstanceOf[Byte])
//          .put(4, (v >>> 24).asInstanceOf[Byte])
//          .put(5, (v >>> 16).asInstanceOf[Byte])
//          .put(6, (v >>> 8).asInstanceOf[Byte])
//          .put(7, (v >>> 0).asInstanceOf[Byte])

  def toBytes(i: Int) = {
    val b = ByteBuffer.allocate(bytesPerInt);
    b.putInt(i);
    b.rewind();
    b
  }
  def fromBytes(bytes: ByteBuffer) = bytes.getInt(0)

  def fromBytes(bytes: Array[Byte]) = ByteBuffer.wrap(bytes).getInt(0)

  def unapply(u: AnyRef) =
    if (u.isInstanceOf[Int])
      Some(u.asInstanceOf[Int])
    else
      None
}

/**
 * Boolean Serializer
 */
case object BooleanSerializer extends Serializer[Boolean] {
  val typeManifest = manifest[Boolean]

	def  toBytes(obj:Boolean):ByteBuffer = {
		    val b:Array[Byte] = Array(1)
		    b(0) =  if(obj == true) 1.asInstanceOf[Byte] else 0.asInstanceOf[Byte]
		    ByteBuffer.wrap(b);
	  }

	  def fromBytes(bytes:ByteBuffer):Boolean =  {
	    if (bytes == null || !bytes.hasArray)
	      null.asInstanceOf[Boolean]
			else
	    	bytes.array()(0) == 1.asInstanceOf[Byte]
	  }

	  def fromBytes(bytes:Array[Byte]):Boolean =  {
	    	bytes(0) == 1.asInstanceOf[Byte]
	  }
	
	  def unapply(u: Any) = 
	    if (u.isInstanceOf[java.lang.Boolean]) 
	      Some(u.asInstanceOf[Boolean])
	    else
	      None
	
}