package org.brzy.calista.ocm

import org.brzy.calista.schema._
import org.scalastuff.scalabeans.Preamble._
import org.brzy.calista.results.ResultSet

/**
 * Document Me..
 * 
 * @author Michael Fortin
 * @version $Id: $
 */
trait Mapping[T<:AnyRef] {

  def newInstance[K: Manifest](key: K): T

  def toColumns(instance: T): List[Column]

  def columnNames:Array[String]


}
