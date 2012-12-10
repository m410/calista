package org.brzy.calista.ocm

/**
 * Thrown when a new instance can not be created
 *
 * @author Michael Fortin
 */
class BuilderInstanceException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)
