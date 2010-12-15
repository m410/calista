package org.brzy.calista.column

import java.util.Date
import org.brzy.calista.schema.Types._

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
case class StandardKey[T](key:T, family:ColumnFamily)(implicit m:Manifest[T]) extends Key {

  def keyBytes = {
    val buf = toBytes(key)
    buf
  }

  def columnPath = ColumnPath(family.name,null,null)

  def |[N,V](key:N,value:V = null,timestamp:Date = new Date())(implicit n:Manifest[N],v:Manifest[V]) =
    Column(key,value,timestamp,this)

  def \\[A<:AnyRef](columns:List[A]) = SlicePredicate(columns,this)
  
  def \[A<:AnyRef](start:A,end:A,count:Int = 100) = SliceRange(start,end,true, count,this)
}