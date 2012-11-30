package org.brzy.calista.dsl

import org.brzy.calista.results.Row

/**
 * ##### Refactored version of the DSL.
 * 
 * @author Michael Fortin
 */
class StandardKey[K](family:StandardFamily, key:K) {

  def apply[C](columnName:C) {}

  def from[C](columnName:C) {}

  def to[C](columnName:C) {}

  def list = List.empty[Row]

  def delete() {}
}
