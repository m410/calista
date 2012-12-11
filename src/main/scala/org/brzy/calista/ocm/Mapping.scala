package org.brzy.calista.ocm

import org.brzy.calista.schema._

/**
 * Document Me..
 * 
 * @author Michael Fortin
 * @version $Id: $
 */
trait Mapping[T<:AnyRef,K] {

  def newInstance(key: K): T

  def toColumns(instance: T): List[Column[_,_]]

  def family:String

  def keyFor(t:T):K
//
//  def superColumn[S](t:T):Option[S]
}
