package org.brzy.calista.schema


/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class StandardFamily( val name:String) extends Family {

  def apply[K](key: K) = new StandardKey(key, this)


  def from[T<:Any:Manifest](key: T)():KeyRange[_,_] = {
    null
  }


  def to[T<:Any:Manifest](key: T):KeyRange[_,_] = {
    null
  }

  override def toString =  "StandardFamily("+name+")"
}


object StandardFamily {
  def apply(name:String) = new StandardFamily(name)
}
