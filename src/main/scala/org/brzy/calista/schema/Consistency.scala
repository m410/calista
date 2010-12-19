package org.brzy.calista.schema

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
/**
 * Document Me..
 *
 * @author Michael Fortin
 */
object Consistency extends Enumeration {
  type Consistency = Value
  val ANY = Value("ANY")
  val ONE = Value("ONE")
  val QUORUM = Value("QUORUM")
  val EACH_QUORUM = Value("EACH_QUORUM")
  val LOCAL_QUORUM = Value("LOCAL_QUORUM") 
  val All = Value("ALL")
}