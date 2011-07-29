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

import org.brzy.calista.Calista
import java.util.Date
import org.brzy.calista.dsl.DslNode


/**
 * Represents only the name part of a column, not the value or timestamp.  It's used to query
 * the datastore.
 * 
 * @author Michael Fortin
 */
case class ColumnName[N:Manifest] protected[schema] (name:N,parent:Key) extends DslNode {
  def nodePath = parent.nodePath + ":Column("+name+")"

  def set[V:Manifest](value:V) {
    val session = Calista.value.get
    session.insert(Column(name,value,new Date(),parent))
  }

  def asColumn = Column(name,null,null, parent)

  override def valueAs[V: Manifest] = {
    val session = Calista.value.get
    val optionRow = session.get(Column(name,null,new Date(),parent))
    optionRow match {
      case Some(row) => Option(row.valueAs[V])
      case _ => None
    }
  }
}