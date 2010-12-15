package org.brzy.calista.column

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
object Conversions {
  implicit def toKeyFamily(str:String) = ColumnFamily(str)
}