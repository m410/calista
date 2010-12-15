package org.brzy.calista.column

import org.brzy.calista.schema.Types

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class SliceRange[T](start: T, finish: T, reverse: Boolean = false, count: Int = 100, key: Key) {
  def startBytes = Types.toBytes(start)

  def finishBytes = Types.toBytes(finish)

  def columnParent: ColumnParent = key match {
    case s: StandardKey[_] => ColumnParent(s.family.name, null)
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.keyBytes)
  }
}