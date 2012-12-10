package org.brzy.calista.schema

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class CounterFamily(val name:String)  extends Family {

  def apply(key: Any) = new CounterKey(key, this)

  def from[T<:Any:Manifest](key: T)():KeyRange[_,_] = {
    null
  }

  def to[T<:Any:Manifest](key: T):KeyRange[_,_] = {
    null
  }

  override def toString =  "CounterFamily("+name+")"
}

/**
 *
 */
object CounterFamily {
  def apply(name:String) = new CounterFamily(name)
}
