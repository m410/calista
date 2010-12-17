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
package	org.brzy.calista.ocm

import org.brzy.calista.column.Column
import org.brzy.calista.column.StandardKey
import org.brzy.calista.schema.Serializer

/**
 * @author Michael Fortin
 */
class ColumnMapping[T<:KeyedEntity[_]:Manifest](overrideFamily:String = null) {

	val family = if(overrideFamily == null)
				manifest[T].erasure.getSimpleName
			else
				overrideFamily
	
	def attributes(serializer:Serializer[_],columns:List[Attribute]):ColumnMapping[T] = {
			this
	}

	def newInstance(columns:List[Column[_,_]]):T  = {
		manifest[T].erasure.newInstance.asInstanceOf[T]
	}

	def toColumns(t:T):List[Column[_,_]] = {
		List.empty[Column[_,_]]
	}

	def toKey(t:T):StandardKey[_] = {
		import org.brzy.calista.column.Conversions._
		family | t.key
	}
}