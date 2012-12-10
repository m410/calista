package org.brzy.calista.schema

import org.brzy.calista.serializer.Serializers._

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class SuperCounterFamily(val name:String)  extends Family{

  def apply[K](key: K) = new SuperCounterKey(key, this)


  override def toString =  "SuperCounterFamily("+name+")"

}


/**
 *
 */
object SuperCounterFamily {
  def apply(name:String) = new SuperCounterFamily(name)
}