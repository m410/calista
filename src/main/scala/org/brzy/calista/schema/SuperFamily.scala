package org.brzy.calista.schema

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
class SuperFamily(val name: String) extends Family {

  def apply[K](key: K) = new SuperKey(key, this)


  override def toString = "SuperFamily(" + name + ")"
}


/**
 *
 */
object SuperFamily {
  def apply(name: String) = new SuperFamily(name)
}