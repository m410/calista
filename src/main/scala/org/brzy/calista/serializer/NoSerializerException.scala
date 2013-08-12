package org.brzy.calista.serializer

/**
 * Thrown when no sutable serializer can be found.
 *
 * @author Michael Fortin
 */
class NoSerializerException(msg: String) extends RuntimeException(msg)
