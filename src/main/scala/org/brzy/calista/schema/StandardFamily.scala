package org.brzy.calista.schema

import org.brzy.calista.serializer.Serializers._


/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class StandardFamily( val name:String) extends Family {

  def apply[K](key: K) = new StandardKey(key, this)

  override def toString =  "StandardFamily("+name+")"
}


object StandardFamily {
  def apply(name:String) = new StandardFamily(name)
}
