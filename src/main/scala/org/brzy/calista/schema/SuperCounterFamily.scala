package org.brzy.calista.schema

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class SuperCounterFamily(val name:String)  extends Family{

  def apply[K](key: K) = new SuperCounterKey(key, this)


  def from[T<:Any:Manifest](key: T)():KeyRange[_,_] = {
    null
  }

  def fromFirst():KeyRange[_,_] = {
    null
  }

  override def toString =  "SuperCounterFamily("+name+")"

}


/**
 *
 */
object SuperCounterFamily {
  def apply(name:String) = new SuperCounterFamily(name)
}