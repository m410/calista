package org.brzy.calista

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

import util.DynamicVariable

/**
 * A Thread Local variable used to access the SessionImpl instance.
 *
 * Note that the Dynamic variable has a null value even thought that's not standard practice
 * in scala.  The reason is that using an Option will create back reference to the class loader
 * and prevent an application deployed in a servlet container from releasing it's resources.
 *
 * @author Michael Fortin
 */
object Calista extends DynamicVariable[Session](null)