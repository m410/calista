/*
 * Copyright 2010 Michael Fortin <mike@brzy.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");  you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed 
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.brzy.calista.server

import org.brzy.calista.{Session, SessionManager, SessionImpl}

/**
 * Used to connect to the embedded server in unit tests.
 *
 * @author Michael Fortin
 */
trait EmbeddedTest {
  val server = EmbeddedServer

  val sessionManager = new SessionManager("Test", "localhost",9161)

  def doWith(f: (Session) => Unit) {
    val session = sessionManager.createSession
    f(session)
    session.close()
  }
}