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

import org.brzy.calista.results.{Column=>RColumn}
import org.brzy.calista.schema.ColumnFamily

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
 *	 entity.insert
 *	}
 * }}}
 *
 * @author Michael Fortin
 */
trait SuperDao[K, S, T <: SuperEntity[K,S]] {
  protected[this] def session = Calista.value.get

	/**
	 * Get an instance of the mapped class by it's key.
	 */
	def apply(key: K)(implicit t: Manifest[K]): T = {
    import org.brzy.calista.schema.Conversions._
    val columns = session.list(mapping.family | key)
    mapping.newInstance(key: K, columns.asInstanceOf[List[RColumn]])
	}

	/**
	 * Optionally get an instance of the mapped class by it's key.
	 */
  def get(key: K, superColumn:S)(implicit k: Manifest[K],s: Manifest[S]): Option[T] = {
    import org.brzy.calista.schema.Conversions._
    val columns = session.list(mapping.family |^ key | superColumn)

    if (columns.size > 0)
      Option(mapping.newInstance(key: K, columns.asInstanceOf[List[RColumn]]))
    else
      None
  }

	/**
	 * List instances of this type by providing a start key and an end key. Note that this may not return
	 * the order that you would expect, depending on the partitioner you are using.
	 *
	 * @param start The first key to return.
	 * @param end The last key to return
	 * @param count The maximum number of results to return.
	 */
  def list(start: K, end: K, count: Int = 100)(implicit t: Manifest[K]):List[T] = {
    val names = mapping.attributes.filter(_.isInstanceOf[Column]).map(_.asInstanceOf[Column].name).toList
    session.keyRange(ColumnFamily(mapping.family).\(start, end, names, count)).map(ks => {
      val keySerializer = mapping.attributes.find(_.isInstanceOf[Key]).get.asInstanceOf[Key].serializer
      mapping.newInstance(keySerializer.fromBytes(ks.key), ks.columns.asInstanceOf[List[RColumn]])
    })
  }

	/**
	 *	This holds implicit function on the entity.  The functions can be called directly on the entity
	 *  eg `entity.insert` etc.
	 */
  class CrudOps(p: T) {
 
		/**
		 * insert the entity
		 */
    def insert = {
      val columns = mapping.toColumns(p)
      columns.foreach(c => session.insert(c))
    }

		/**
		 * remove the entity
		 */
    def remove = {
      val key = mapping.toKey(p)
      session.remove(key)
    }
  }

	/**
	 * Apply the operations to the entity.
	 */
  implicit def applyCrudOps(p: T): CrudOps = new CrudOps(p)

	/**
	 * This needs to be implemented for each instance to define the mapping to the cassandra datastore.
	 */
  def mapping: Mapping[T]

}