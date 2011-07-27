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