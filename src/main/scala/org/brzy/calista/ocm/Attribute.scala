package org.brzy.calista.ocm

import org.brzy.calista.schema.Serializer
import org.brzy.calista.schema.Utf8Type

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
case class Attribute(name:String, serializer:Serializer[_] = Utf8Type)