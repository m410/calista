package org.brzy.calista.ocm

import org.brzy.calista.serializer.{UTF8Serializer, Serializers, Serializer}
import reflect._

/**
 * Document Me..
 *
 * @author Michael Fortin
 * @version $Id: $
 */
class StandardMappingBuilder[ K: ClassTag, T <: AnyRef : ClassTag](
        family: Option[String] = None,
        key: Option[ColDef[_]] = None,
        columns: Seq[ColDef[_]] = Seq.empty[ColDef[_]]) {

  def family(name: String) = {
    new StandardMappingBuilder[K,T](
      family = Option(name),
      key = key,
      columns = columns
      )
  }

  def key[S](name: String, serializer: Serializer[S] = UTF8Serializer) = {
    new StandardMappingBuilder[K,T](
      family = family,
      key = Option(ColDef(name,serializer)),
      columns = columns
    )
  }

  def column[C](name: String, serializer: Serializer[C] = UTF8Serializer) = {
    new StandardMappingBuilder[K,T](
      family = family,
      key = key,
      columns = columns ++ Seq(ColDef(name,serializer))
    )
  }

  def make: StandardMapping[K,T] = {
    new ReflectionMapping[K,T](
        family = family.getOrElse(throw new InvalidMappingException("No Family Defined",null)),
        keyCol = key.getOrElse(throw new InvalidMappingException("No Key Defined",null)),
        columns = columns
    )
  }
}
