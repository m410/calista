package org.brzy.calista.ocm

import org.brzy.calista.serializer.{UTF8Serializer,  Serializer}
import org.brzy.calista.results.ResultSet

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class ReflectMapping[T<:AnyRef:Manifest] protected[ocm] (
        val columnFamilyName:String,
        val columnNameSerializer: Serializer[_] = UTF8Serializer,
        val superColumnKey: Option[MappingAttribute] = None,
        val primaryKey: Option[MappingAttribute] = None,
        val attributes: Seq[MappingAttribute] = Seq.empty[MappingAttribute])
  extends Mapping[T]{

  def columnNameSerializer(s:Serializer[_]) = {
    new ReflectMapping[T](
      columnFamilyName = columnFamilyName,
      columnNameSerializer = s,
      superColumnKey = superColumnKey,
      primaryKey = primaryKey,
      attributes = attributes)
  }

  def field(s:MappingAttribute) = {
    new ReflectMapping[T](
      columnFamilyName = columnFamilyName,
      columnNameSerializer = columnNameSerializer,
      superColumnKey = superColumnKey,
      primaryKey = primaryKey,
      attributes = attributes ++ Seq(s))
  }

  def key(s:MappingAttribute) = {
    new ReflectMapping[T](
      columnFamilyName = columnFamilyName,
      columnNameSerializer = columnNameSerializer,
      superColumnKey = superColumnKey,
      primaryKey = Option(s),
      attributes = attributes)
  }

  def superColumn(s:MappingAttribute) = {
    new ReflectMapping[T](
      columnFamilyName = columnFamilyName,
      columnNameSerializer = columnNameSerializer,
      superColumnKey = Option(s),
      primaryKey = primaryKey,
      attributes = attributes)
  }

  def newInstance[K: Manifest](key: K) = {

    null.asInstanceOf[T]
  }

  def toColumns(instance: T) = null

  def columnNames = null
}

object ReflectMapping {
  def apply[T<:AnyRef:Manifest](columnFamilyName:String) = {
    new ReflectMapping[T]( columnFamilyName)
  }
}
