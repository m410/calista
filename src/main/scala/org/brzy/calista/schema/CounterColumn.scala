package org.brzy.calista.schema

import java.util.Date

/**
 * This represents the counter column when querying cassandra
 * 
 * @author Michael Fortin
 */
protected class CounterColumn[K:Manifest](name: K, value: Long, timestamp: Date, parent: Key) {

}