package org.brzy.calista.server

import org.brzy.calista.{SessionManager, Session}

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
trait EmbeddedTest {
  val server = EmbeddedServer
  val sessionManager = new SessionManager()

  def doWith(f: (Session) => Unit) = {
    val session = sessionManager.createSession
    f(session)
    session.close
  }
}