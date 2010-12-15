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