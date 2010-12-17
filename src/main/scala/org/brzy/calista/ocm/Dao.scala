package org.brzy.calista.ocm

import org.brzy.calista.column.Column

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
trait Dao[K,T<:KeyedEntity[K]] {
	def session = Calista.value.get
	
	def get(key:K)(implicit t:Manifest[K]):T = {
		import org.brzy.calista.column.Conversions._
		val columns = session.get(columnMapping.family | key)
		columnMapping.newInstance(columns.asInstanceOf[List[Column[_,_]]])
	}
	
	class CrudOps(p:T) {
		def save = {
			val columns = columnMapping.toColumns(p)
			columns.foreach(c=>session.insert(c))
		}
		def remove = {
			val key = columnMapping.toKey(p)
			session.remove(key)
		}
	}
	
	implicit def applyCrudOps(p:T):CrudOps = new CrudOps(p)

  def columnMapping:ColumnMapping[T]
}