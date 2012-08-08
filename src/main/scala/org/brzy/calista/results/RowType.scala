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
package org.brzy.calista.results

import org.scalastuff.scalabeans.Enum
import org.apache.commons.lang.builder.ToStringBuilder

/**
 * Marks each return row in the resultSet as the type of row it is in the data store.
 * 
 * @author Michael Fortin
 */
class RowType private (val name:String) {
  override def toString = new ToStringBuilder(this).append(name).toString
}

object RowType extends Enum[RowType] {

  /**
   * Standard Column Family.  For rows of this type, the superColumn will be null.
   */
  val Standard = new RowType("Standard")

  /**
   * Standard Column Family with a CounterColumnType default validation class.  For rows of this
   * type, the superColumn will be null, the timestamp will be null and the value will always be
   * a long type.
   */
  val StandardCounter = new RowType("StandardCounter")

  /**
   * Super Column Family. Rows of this type will have all fields available.
   */
  val Super = new RowType("Super")

  /**
   * Super Column Family with a CounterColumnType default validation class. Rows of this type will
   * have all fields available and the value will be a Long type.
   */
  val SuperCounter = new RowType("SuperCounter")
}