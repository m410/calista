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

/**
 * Marks each return row in the result set as the type of row it is in the data store.
 * 
 * @author Michael Fortin
 */
class RowType private ()
object RowType extends Enum[RowType] {
  val Empty = new RowType
  val Standard = new RowType
  val StandardCounter = new RowType
  val Super = new RowType
  val SuperCounter = new RowType
}