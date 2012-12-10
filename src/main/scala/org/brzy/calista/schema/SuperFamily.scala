package org.brzy.calista.schema

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class SuperFamily(val name:String)  extends Family{

  def apply(key: Any) = new SuperKey(key, this)


  def from[T<:Any:Manifest](key: T)():KeyRange[_,_] = {
    null
  }

  def fromFirst():KeyRange[_,_] = {
    null
  }

  override def toString =  "SuperFamily("+name+")"
}


/**
 *
 */
object SuperFamily {
  def apply(name:String) = new SuperFamily(name)
}