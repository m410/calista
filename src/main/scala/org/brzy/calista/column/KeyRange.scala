package org.brzy.calista.column

import org.brzy.calista.schema.Types

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
case class KeyRange[T,C](
        start:T,
        finish:T,
        predicate:SlicePredicate[C],
        columnFamily:ColumnFamily,
        count:Int = 100) {
  def startBytes =
    if(start != null)
      Types.toBytes(start)
    else
      null
  def finishBytes =
    if(finish != null)
      Types.toBytes(finish)
    else
      null
  def columnParent = ColumnParent(columnFamily.name, null)
}