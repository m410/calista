package org.brzy.calista.column

import java.nio.ByteBuffer
import java.util.Date
import org.brzy.calista.schema.{Types, Utf8Type}

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class Column[K, V](name: K, value: V, timestamp: Date, parent: Key)(implicit k: Manifest[K], v: Manifest[V])
        extends ColumnOrSuperColumn {
  def nameBytes = Types.toBytes(name)

  def valueBytes = Types.toBytes(value)

  def columnPath = {
    val superCol = parent match {
      case s: SuperColumn[_] => s.keyBytes
      case _ => null
    }
    ColumnPath(parent.family.name, superCol, nameBytes)
  }

  def columnParent: ColumnParent = parent match {
    case s: StandardKey[_] => ColumnParent(s.family.name, null)
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.keyBytes)
//    case s: SuperColumn[_] => ColumnParent(s.family.name, s.superKey.keyBytes)
  }
}