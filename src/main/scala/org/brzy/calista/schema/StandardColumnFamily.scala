package org.brzy.calista.schema


/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class StandardColumnFamily( val name:String) extends ColumnFamily {

  def apply(key: Any) = new StandardKey(key, this)


  def from[T<:Any:Manifest](key: T)():KeyRange[_,_] = {
    null
  }


  def to[T<:Any:Manifest](key: T):KeyRange[_,_] = {
    null
  }


}


object StandardColumnFamily {
  def apply(name:String) = new StandardColumnFamily(name)
}
