package org.brzy.calista.results

import org.scalastuff.scalabeans.Enum

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class RowType private ()
object RowType extends Enum[RowType] {
  val Standard = new RowType
  val Super = new RowType
  val Counter = new RowType
}