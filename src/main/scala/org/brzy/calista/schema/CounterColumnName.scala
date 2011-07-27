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

import java.util.Date
import io.BytePickle.Def
import org.brzy.calista.ocm.Calista
import org.brzy.calista.Session
import com.sun.xml.internal.ws.developer.MemberSubmissionAddressing.Validation
import org.brzy.calista.results.Row

/**
 * This represents the counter column when querying cassandra
 * 
 * @author Michael Fortin
 */
protected case class CounterColumnName[K:Manifest](name: K,parent:Key) extends DslNode {
  def nodePath = parent.nodePath +":Counter("+name+")"


  override def -=(amount: Long) {
    set(-amount)
  }

  override def +=(amount: Long) {
    set(amount)
  }

  def set(amount: Long) {
    val session = Calista.value.asInstanceOf[Session]
    session.add(Column(name, amount, new Date(), parent))
  }

  override def count = {
    val session = Calista.value.asInstanceOf[Session]
    val optionReturnColumn = session.get(Column(name,null,new Date(),parent))

    0
  }
}