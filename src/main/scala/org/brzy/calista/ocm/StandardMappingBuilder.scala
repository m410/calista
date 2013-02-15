package org.brzy.calista.ocm

import org.brzy.calista.serializer.{UTF8Serializer, Serializers, Serializer}
import reflect._

/**
 * Document Me..
 *
 * @author Michael Fortin
 * @version $Id: $
 */
class StandardMappingBuilder[T <: AnyRef : ClassTag, K: ClassTag](family: String, key: ColDef[_], columns: Seq[ColDef[_]] = Seq.empty[ColDef[_]]) {

  def family(name: String) = {
    new StandardMappingBuilder[T,K](
      family = name,
      key = key,
      columns = columns
      )
  }

  def key[S](name: String, serializer: Serializer[S] = UTF8Serializer) = {
    new StandardMappingBuilder[T,K](
      family = name,
      key = ColDef(name,serializer),
      columns = columns
    )
  }

  def column[C](name: String, serializer: Serializer[C] = UTF8Serializer) = {
    new StandardMappingBuilder[T,K](
      family = name,
      key = ColDef(name,serializer),
      columns = columns ++ Seq(ColDef(name,serializer))
    )
  }

  def make: StandardMapping[T, K] = {
    new ReflectionMapping[T,K](
        family = family,
        keyCol = key,
        columns = columns
    )
  }
}
