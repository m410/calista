package org.brzy.calista.results

import java.nio.ByteBuffer
import org.brzy.calista.schema.Serializer
import org.brzy.calista.column.ColumnOrSuperColumn

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class SuperColumn[T](bytes: ByteBuffer, serializer: Serializer[T], columns: List[Column])
        extends ColumnOrSuperColumn {
  def key: T = serializer.fromBytes(bytes)
}