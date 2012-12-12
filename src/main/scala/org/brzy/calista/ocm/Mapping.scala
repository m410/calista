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

import org.brzy.calista.schema._

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
trait Mapping[T<:AnyRef,K] {

  def newInstance(key: K): T

  def toColumns(instance: T): List[Column[_,_]]

  def family:String

  def keyFor(t:T):K
//
//  def superColumn[S](t:T):Option[S]
}
