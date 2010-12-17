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

import org.brzy.calista.column.Column

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
trait Dao[K,T<:KeyedEntity[K]] {
	def session = Calista.value.get
	
	def get(key:K)(implicit t:Manifest[K]):T = {
		import org.brzy.calista.column.Conversions._
		val columns = session.get(columnMapping.family | key)
		columnMapping.newInstance(columns.asInstanceOf[List[Column[_,_]]])
	}
	
	class CrudOps(p:T) {
		def save = {
			val columns = columnMapping.toColumns(p)
			columns.foreach(c=>session.insert(c))
		}
		def remove = {
			val key = columnMapping.toKey(p)
			session.remove(key)
		}
	}
	
	implicit def applyCrudOps(p:T):CrudOps = new CrudOps(p)

  def columnMapping:ColumnMapping[T]
}