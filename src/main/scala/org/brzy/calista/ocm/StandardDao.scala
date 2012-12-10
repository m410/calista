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

/**
 * Data Access Object.  Companion objects of persistable classes need to extend this.  It adds
 * the basic functionality need to access the data store.
 *
 * {{{
 * case class Entity(id:Long, name:String) extends KeyedEntity[Long]
 * object Entity extends Dao[Entity] { ...}
 *
 * SessionManager.doWith {session =>
 *   val entity = Entity(1,"bob")
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
    val columns = StandardFamily(mapping.family)(key).list
    mapping.newInstance(key, None, columns)
  }

  /**
   * Optionally get an instance of the mapped class by it's key.
   */
  def get(key: K)(implicit t: Manifest[K]): Option[T] = {
    val columns = StandardFamily(mapping.family)(key).list

    if (columns.size > 0)
      Option(mapping.newInstance(key, None, columns))
    else
      None
  }


  /**
   * This holds implicit function on the entity.  The functions can be called directly on the entity
   * eg `entity.insert` etc.
   */
  class CrudOps(p: T) {

    /**
     * insert the entity
     */
    def insert() = {
      val columns = mapping.toColumns(p)
      columns.foreach(c => session.insert(c))
      p
    }

    /**
     * remove the entity
     */
    def remove() {
      val key = mapping.toKey(p).right.get
      session.remove(key)
    }
  }

  /**
   * Apply the operations to the entity.
   */
  implicit def applyCrudOps(p: T): CrudOps = new CrudOps(p)

  /**
   * This needs to be implemented for each instance to define the mapping to the
   * cassandra datastore.
   */
  def mapping: BeanMapping[T]

}