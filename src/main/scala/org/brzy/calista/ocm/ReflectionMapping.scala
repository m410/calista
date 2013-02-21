package org.brzy.calista.ocm

import org.brzy.calista.schema.{Column, StandardFamily}
import org.brzy.beanwrap.Builder
import scala.reflect.runtime.universe._
import reflect._

import org.slf4j.LoggerFactory

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class ReflectionMapping[K:ClassTag, T<:AnyRef:ClassTag](
        val family:String,
        val keyCol:ColDef[_],
        val columns:Seq[ColDef[_]]
    ) extends StandardMapping[K,T]{

  private[this] val log = LoggerFactory.getLogger(getClass)

  def newInstance(key: K) = {
    val rows = StandardFamily(family)(key).list
    val builder = rows.foldLeft(Builder[T]())((builder,row)=>{
      val columnName = row.columnAs[String]
      columns.find(_.name == columnName)  match {
        case Some(column) =>
          val value = column.serializer.fromBytes(row.value)
          builder.set(columnName->value)
        case None =>
          log.warn("Unknown Column mapping: {} on {}", columnName, classTag[T].runtimeClass )
          builder
      }
    })

    builder.set(keyCol.name -> keyCol.serializer.fromBytes(rows.head.key)).make
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

  def keyFor(t: T):K = {
    null.asInstanceOf[K]
  }
}
