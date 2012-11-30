package org.brzy.calista.dsl

import org.brzy.calista.Calista

/**
 * ##### Refactored version of the DSL.
 *
 * Represents a Column Family in the cassandra data store.
 * 
 * @author Michael Fortin
 */
class StandardFamily(name:String) {

  /**
   * Will return a standard column key or super column key depending on the type of
   * column family
   * @param key the column key value, typically a UUID, Long or String.
   */
  def apply(key:Any) = new StandardKey(this, key)

  /**
   *
   * @return
   * @throws NoSuchElementException when no family exists with this name
   */
  def definition = Calista.value.ksDef.families.find(_.name == name)
          .getOrElse(throw new NoSuchElementException())


}

/**
 *
 */
object StandardFamily {
  def apply(name:String) = new StandardFamily(name)
}