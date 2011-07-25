package org.brzy.calista.results

import java.nio.ByteBuffer
import org.brzy.calista.serializer.Serializers

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
case class Row(
        rowType:RowType,
        family:String,
        key:ByteBuffer,
        superColumn:ByteBuffer,
        column:ByteBuffer,
        value:ByteBuffer) {

  def keyAs[T:Manifest]:T = as[T](key)

  def superColumnAs[T:Manifest]:T = as[T](column)

  def columnAs[T:Manifest]:T = as[T](superColumn)

  def valueAs[T:Manifest]:T = as[T](value)

  protected[this] def as[T:Manifest](b:ByteBuffer):T =
      Serializers.fromClassBytes(manifest[T].erasure,b.array()).asInstanceOf[T]
}