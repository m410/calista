package org.brzy.calista.system

import org.scalastuff.scalabeans.Enum
import org.apache.cassandra.thrift.{ColumnDef,IndexType=>CIndexType}

/**
 * Column Definition
 *
 * {{{
 * struct ColumnDef {
 *     1: required binary name,
 *     2: required string validation_class,
 *     3: optional IndexType index_type,
 *     4: optional string index_name
 * }
 * }}}
 * 
 * @author Michael Fortin
 */
case class ColumnDefinition(
    name:Array[Byte],
    validationClass:String,
    indexType:Option[IndexType],
    indexName:Option[String]) {

  def asColumnDef = {
    val cDef = new ColumnDef()
    cDef.setName(name)
    cDef.setValidation_class(validationClass)
    if (indexType.isDefined) cDef.setIndex_type(CIndexType.KEYS)
    if (indexName.isDefined) cDef.setIndex_name(indexName.get)
    cDef
  }
}

object ColumnDefinition {
  def apply(d:ColumnDef) = {
    new ColumnDefinition(
      name = d.getName,
      validationClass = d.getValidation_class,
      indexType = if(d.getIndex_type == null) None else Option(IndexType.KEYS),
      indexName = Option(d.getIndex_name))
  }
}

class IndexType private()
object IndexType extends Enum[IndexType] {
  val KEYS = new IndexType
}