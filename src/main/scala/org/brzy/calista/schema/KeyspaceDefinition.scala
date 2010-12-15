package org.brzy.calista.schema

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
case class KeyspaceDefinition(
    name:String,
    strategyClass:String,
    replicationFactor:Int,
    families:List[FamilyDefinition])