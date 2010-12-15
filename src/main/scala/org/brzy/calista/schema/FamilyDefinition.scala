package org.brzy.calista.schema

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
case class FamilyDefinition(
    name:String,
    columnType:String,
    comparatorType:String,
    subcomparatorType:String,
    comment:String,
    rowCacheSize:Double,
    keyCacheSize:Double)