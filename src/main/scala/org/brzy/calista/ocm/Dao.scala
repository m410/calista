package org.brzy.calista.ocm

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
trait Dao[K<:KeyedEntity] {
  def get(key:String):K

  class CrudOps(t:K) {
    def save = {}

    def remove = {}
  }

  implicit def applyCrudOps(t:K) = new CrudOps(t)

  val ocm:ColumnMapping
}