package org.brzy.calista.column

import java.nio.ByteBuffer
import java.util.Date
import org.brzy.calista.schema.Types

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class SuperColumn[T](key: T, superKey: SuperKey[_])(implicit m: Manifest[T])
        extends Key with ColumnOrSuperColumn {

  def |[N, V](sKey: N, value: V = null, timestamp: Date = new Date())(implicit n: Manifest[N], v: Manifest[V]) =
    Column(sKey, value, timestamp, this)

  def keyBytes = Types.toBytes(key)

  def family = superKey.family

  def columnPath = ColumnPath(superKey.family.name,keyBytes,null)
}