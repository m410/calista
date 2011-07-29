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
package org.brzy.calista.system

import org.apache.cassandra.thrift.KsDef
import collection.JavaConversions._

/**
 * A description of the Keyspace and all it's attributes and elements.
 *
 * This is just a proxy to the underlying casandra thrift object which looks like this:
 * {{{
 * struct KsDef {
 *     1: required string name,
 *     2: required string strategy_class,
 *     3: optional map<string,string> strategy_options,
 *
 *     /** @deprecated */
 *     4: optional i32 replication_factor,
 *
 *     5: required list<CfDef> cf_defs,
 *     6: optional bool durable_writes=1,
 * }
 * }}}
 * 
 * @see SessionManager.keyspaceDefinition
 * @author Michael Fortin
 */
case class KeyspaceDefinition(
    name:String,
    strategyClass:String,
    strategyOptions:Option[Map[String,String]] = None,
    families:List[FamilyDefinition],
    durableWrites:Boolean = true)


object KeyspaceDefinition {
  def apply(kdef:KsDef) = {
    new KeyspaceDefinition(
      name= kdef.getName,
      strategyClass = kdef.getStrategy_class,
      families = kdef.getCf_defs.map(c=>FamilyDefinition(c)).toList)
  }
}