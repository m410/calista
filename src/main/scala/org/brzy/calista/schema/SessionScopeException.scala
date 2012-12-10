package org.brzy.calista.schema

/**
 * Thrown where there is no active session.
 *
 * @author Michael Fortin
 */
class SessionScopeException(msg: String, e: Throwable) extends RuntimeException(msg, e)