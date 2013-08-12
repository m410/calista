package org.brzy.calista.schema

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
class CounterFamily(val name: String) extends Family {

  def apply[K](key: K) = new CounterKey(key, this)


  override def toString = "CounterFamily(" + name + ")"
}

/**
 *
 */
object CounterFamily {
  def apply(name: String) = new CounterFamily(name)
}
