package org.brzy.calista.system

import org.scalastuff.scalabeans.Enum


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
class ColumnDefinition(
    name:Array[Byte],
    validationClass:String,
    indexType:Option[IndexType],
    indexName:Option[String])

class IndexType private()
object IndexType extends Enum[IndexType] {
  val KEYS = new IndexType
}