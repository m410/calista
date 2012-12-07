package org.brzy.calista.schema

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class SuperColumnFamily(val name:String)  extends ColumnFamily{

  def apply(key: Any) = new SuperKey(key, this)


  def from[T<:Any:Manifest](key: T)():KeyRange[_,_] = {
    null
  }

  def fromFirst():KeyRange[_,_] = {
    null
  }

}


/**
 *
 */
object SuperColumnFamily {
  def apply(name:String) = new SuperColumnFamily(name)
}