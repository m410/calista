package org.brzy.calista.column

import org.brzy.calista.schema.Types

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
case class SuperKey[T](key:T,family:ColumnFamily)(implicit t:Manifest[T]) {
  def |[N](sKey:N)(implicit n:Manifest[N]) = SuperColumn(sKey,this)
  def keyBytes = {
    val buf = Types.toBytes(key)
    buf
  }
}