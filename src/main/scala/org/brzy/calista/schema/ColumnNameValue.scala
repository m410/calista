package org.brzy.calista.schema

import org.brzy.calista.serializer.Serializers
import java.nio.ByteBuffer
import java.util.Date

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
@deprecated
trait ColumnNameValue[N,V] {

  def value:V
  def name:N
  def timestamp:Date
  def parent: Key

  /**
	 * Return the name converted to bytes.
	 */
  def nameBytes = Serializers.toBytes(name)

	/**
	 * Return the value converted to bytes.
	 */
  def valueBytes = Serializers.toBytes(value)

	/**
	 * Used by the Session object for querying.  Uses of the column class should not have to use this method
	 * directly.
	 */
  def columnPath = {
    val superCol = parent match {
      case s: SuperColumn[_] => s.keyBytes
      case _ => null
    }
    ColumnPath(parent.family.name, superCol, nameBytes)
  }

	/**
	 * Used by the Session object for querying.  Uses of the column class should not have to use this method
	 * directly.
	 */
  def columnParent: ColumnParent = parent match {
    case s: StandardKey[_] => ColumnParent(s.family.name, null)
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.keyBytes)
  }

  def insert() {

  }

}