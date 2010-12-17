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

import java.util.{UUID => JUUID}
import _root_.com.eaio.uuid.{UUID => EUUID}


object UUID {


  def apply() = JUUID.fromString(new EUUID().toString());

  def apply(data:Array[Byte]):JUUID = {
    var msb = 0L
    var lsb = 0L
    assert(data.length == 16)

    (0 until 8).foreach  { (i) => msb = (msb << 8) | (data(i) & 0xff) }
    (8 until 16).foreach { (i) => lsb = (lsb << 8) | (data(i) & 0xff) }

    JUUID.fromString(new EUUID(msb,lsb).toString)
  }
}