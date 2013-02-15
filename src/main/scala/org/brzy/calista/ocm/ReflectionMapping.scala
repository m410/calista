package org.brzy.calista.ocm

import reflect.ClassTag
import org.brzy.calista.schema.{Column, StandardFamily}
import org.brzy.beanwrap.Builder
import scala.reflect.runtime.universe._

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class ReflectionMapping[T<:AnyRef:ClassTag,K:ClassTag](
        val family:String,
        val keyCol:ColDef[_],
        val columns:Seq[ColDef[_]]
    ) extends StandardMapping[T,K]{

  def newInstance(key: K) = {
    val rows = StandardFamily(family)(key).list
    val builder = rows.foldLeft(Builder[T]())((builder,row)=>{
      val columnName = row.columnAs[String]
      val value = columns.find(_.name == columnName).get.serializer.fromBytes(row.value)
      builder.set(columnName->value)
    })

    builder.set(keyCol.name -> keyCol.serializer.fromBytes(rows.head.key))

    builder.make
  }

  def toColumns(instance: T)(implicit t:TypeTag[T]) = {
    val keyTerm = t.tpe.member(newTermName(keyCol.name)).asMethod
    val key = rootMirror.reflect(instance).reflectMethod(keyTerm)()

    columns.map(f=>{
      val keyTerm = t.tpe.member(newTermName(f.name)).asMethod
      val value = rootMirror.reflect(instance).reflectMethod(keyTerm)()
      StandardFamily(family)(key).column(f.name, value)
    }).toList
  }

  def keyFor(t: T) = {
    null.asInstanceOf[K]
  }
}
