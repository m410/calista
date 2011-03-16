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

/**
 * This needs to be applied to the persisted entity, so that the key column is defined.  This 
 * is necessary for the Dao.get(key:PK) and a few other functions.
 * 
 * @author Michael Fortin
 */
trait StandardEntity[T] {
	def key:T
}

/**
 * 
 */
trait SuperEntity[T,S] extends StandardEntity[T] {
  def superColumn:S
}