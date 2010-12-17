package	org.brzy.calista.ocm

import org.brzy.calista.column.Column
import org.brzy.calista.column.StandardKey
import org.brzy.calista.schema.Serializer

/**
 * @author Michael Fortin
 */
class ColumnMapping[T<:KeyedEntity[_]:Manifest](val family:String) {
	
	def attributes(serializer:Serializer[_],columns:List[Attribute]):ColumnMapping[T] = {
			this
	}

	def newInstance(columns:List[Column[_,_]]):T  = {
		manifest[T].erasure.newInstance.asInstanceOf[T]
	}

	def toColumns(t:T):List[Column[_,_]] = {
		List.empty[Column[_,_]]
	}

	def toKey(t:T):StandardKey[_] = {
		import org.brzy.calista.column.Conversions._
		family | t.key
	}
}