package org.brzy.calista.results

import java.nio.ByteBuffer
import java.util.Date
import org.brzy.calista.column.ColumnOrSuperColumn
import org.brzy.calista.schema.Serializer

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class Column(name: Array[Byte], value: Array[Byte], timestamp: Date) extends ColumnOrSuperColumn {
  def nameAs[T](s:Serializer[T]):T = s.fromBytes(name)
  def valueAs[T](s:Serializer[T]):T = s.fromBytes(value)
}