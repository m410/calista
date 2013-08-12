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
package org.brzy.calsta

/**
 * Holds the structure of the cassandra datamodel.  In most cases the classes in this package
 * map directly to the classes in the Thrift cassandra client api but with a more hierarchical
 * layout.  The classes are for querying not reading results.
 *
 * @author Michael Fortin
 */
package object schema 