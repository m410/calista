package org.brzy.calista.results

import java.nio.ByteBuffer
import org.brzy.calista.schema.ColumnOrSuperColumn
import org.brzy.calista.serializer.Serializer

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
case class KeySlice(key:Array[Byte],columns:List[ColumnOrSuperColumn]) {
  def keyAs[T](s:Serializer[T]):T = s.fromBytes(key)
}