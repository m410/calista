package org.brzy.calista.dsl

import org.brzy.calista.schema.ColumnFamily

/**
 * Document Me..
 *
 * @author Michael Fortin
 * @version $Id: $
 */

object CounterColumn {

  def apply[K:Manifest,N:Manifest](name:String)(key:K)(columnName:N) = {
    new ColumnFamily(name).standardKey(key).counter(columnName)
  }
}
