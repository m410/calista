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