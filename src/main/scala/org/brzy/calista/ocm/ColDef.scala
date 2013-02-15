package org.brzy.calista.ocm

import org.brzy.calista.serializer.Serializer

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class ColDef[S](name: String, serializer: Serializer[S])
