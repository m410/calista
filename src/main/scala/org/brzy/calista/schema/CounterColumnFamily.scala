package org.brzy.calista.schema

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class CounterColumnFamily(val name:String)  extends ColumnFamily{

  def apply(key: Any) = new CounterKey(key, this)


  def from[T<:Any:Manifest](key: T)():KeyRange[_,_] = {
    null
  }

  def to[T<:Any:Manifest](key: T):KeyRange[_,_] = {
    null
  }
}

object CounterColumnFamily {
  def apply(name:String) = new CounterColumnFamily(name)
}
