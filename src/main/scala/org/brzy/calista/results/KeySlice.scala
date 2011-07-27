package org.brzy.calista.results

import org.brzy.calista.schema.ColumnOrSuperColumn
import org.brzy.calista.serializer.Serializer

/**
 * Represents the result of a query that returns a slice.  This contains the key and
 * the columns of the slice.
 * 
 * @author Michael Fortin
 */
@deprecated
case class KeySlice(key:Array[Byte],columns:List[ColumnOrSuperColumn]) {
  def keyAs[T](s:Serializer[T]):T = s.fromBytes(key)
}