package org.brzy.calista.dsl

import org.brzy.calista.schema.ColumnFamily

/**
 * Document Me..
 *
 * @author Michael Fortin
 * @version $Id: $
 */

object CounterSuperColumn {

  def apply[K:Manifest,S:Manifest,N:Manifest](name:String)(key:K, superKey:S)(columnName:N) = {
    new ColumnFamily(name).superKey(key).superColumn(superKey).counter(columnName)
  }
}
