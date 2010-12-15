package org.brzy.calista.column

import java.nio.ByteBuffer
import org.brzy.calista.schema.Types

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class SlicePredicate[T](columns: List[T], key: Key) {
  def toByteList = columns.map(c => Types.toBytes(c))

  def columnParent: ColumnParent = key match {
    case s: StandardKey[_] => ColumnParent(s.family.name, null)
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.keyBytes)
  }
}
