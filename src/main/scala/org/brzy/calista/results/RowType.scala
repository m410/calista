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


/**
 * The different Column family types.
 */
object RowType extends Enumeration {
  type RowType = Value

  /**
   * Standard Column Family.  For rows of this type, the superColumn will be null.
   */
  val Standard = Value("Standard")

  /**
   * Standard Column Family with a CounterColumnType default validation class.  For rows of this
   * type, the superColumn will be null, the timestamp will be null and the value will always be
   * a long type.
   */
  val StandardCounter = Value("StandardCounter")

  /**
   * Super Column Family. Rows of this type will have all fields available.
   */
  val Super = Value("Super")

  /**
   * Super Column Family with a CounterColumnType default validation class. Rows of this type will
   * have all fields available and the value will be a Long type.
   */
  val SuperCounter = Value("SuperCounter")
}