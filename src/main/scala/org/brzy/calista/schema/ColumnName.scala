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
package org.brzy.calista.schema

import org.brzy.calista.ocm.Calista
import org.brzy.calista.Session
import java.util.Date
import org.brzy.calista.results.Row

/**
 * Document Me..
 * 
 * @author Michael Fortin
 * @version $Id: $
 */

protected case class ColumnName[N:Manifest](name:N,parent:Key) extends DslNode {
  def nodePath = parent.nodePath + ":Column("+name+")"

  override def <=[V: Manifest](value: V) {
    set(value)
  }

  def set[V:Manifest](value:V) {
    val session = Calista.value.asInstanceOf[Session]
    session.insert(Column(name,value,new Date(),parent))
  }

  override def as[V: Manifest] = {
    val session = Calista.value.asInstanceOf[Session]
    val optionColumn = session.get(Column(name,null,new Date(),parent))
    null.asInstanceOf[V]
  }
}