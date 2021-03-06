/*
 * Copyright 2010 Michael Fortin <mike@brzy.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");  you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed 
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.brzy.calista.ocm

import org.brzy.calista.schema.{StandardFamily, Family}
import org.brzy.calista.Calista

import scala.reflect.runtime.universe.TypeTag

import scala.language.implicitConversions

/**
 * Data Access Object.  Companion objects of persistable classes need to extend this.  It adds
 * the basic functionality need to access the data store.
 *
 * {{{
 * case class Entity(id:Long, name:String) extends KeyedEntity[Long]
 * class EntityStore extends StandardDao[Entity] { ...}
 *
 * SessionManager.doWith {session =>
 *   val entityStore = new EntityStore
 *   val entity = entityStore(1,"bob")
 * 	 entity.insert
 * 	}
 * }}}
 *
 * @author Michael Fortin
 */
trait StandardDao[K, T <: AnyRef] {
  protected[this] def session = Calista.value

  /**
   * Get an instance of the mapped class by it's key.
   */
  def apply(key: K)(implicit t: Manifest[K]): T = {
    mapping.newInstance(key)
  }

  /**
   * Optionally get an instance of the mapped class by it's key.
   */
  def get(key: K)(implicit t: Manifest[K]): Option[T] = {
    val columns = StandardFamily(mapping.family)(key).list

    if (columns.size > 0)
      Option(mapping.newInstance(key))
    else
      None
  }


  /**
   * This holds implicit function on the entity.  The functions can be called directly on the entity
   * eg `entity.insert` etc.
   */
  class CrudOps(p: T)(implicit t:TypeTag[T]) {

    /**
     * insert the entity
     */
    def insert() = {
      mapping.toColumns(p).foreach(c => session.insert(c))
      p
    }

    /**
     * remove the entity
     */
    def remove() {
      StandardFamily(mapping.family)(mapping.keyFor(p)).remove()
    }
  }

  /**
   * Apply the operations to the entity.
   */
  implicit def applyCrudOps(p: T)(implicit t:TypeTag[T]): CrudOps = new CrudOps(p)

  /**
   * This needs to be implemented for each instance to define the mapping to the
   * cassandra datastore.
   */
  def mapping: StandardMapping[K,T]

}