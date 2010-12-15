package org.brzy.calista

import column._
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._
import java.util.{UUID=>JUUID}
import schema.UUID


class UsageTest extends JUnitSuite {
  @Test def testDsl = {
    import Conversions._

    val standardKey:StandardKey[String] =
        "Family" | "key"
    
    val standardKeyWithUUID:StandardKey[JUUID] =
        "Family" | UUID()

    val standardKeyWithLong:StandardKey[Long] =
        "Family" | 22343L

    val standardColumn:Column[String,String] =
        "Family" | "key" |("column", "value")
//    val standardColumn = ColumnFamily("Family").key("key").column("column", "value")

    val standardKeyRange:KeyRange[String,String] =
        "Family" \("start","end",List.empty[String],20)

    val standardSliceRange:SliceRange[String] =
        {"Family" | "key"}\("start","finish")

    val standardSlicePredicate:SlicePredicate[String] =
        {"Family"|"key"}\\List("column","column3","column3")
//    val ssp2 = ColumnFamily("Family").key("key").predicate(List("column","column3","column3"))


    val superKey:SuperKey[String] =
        "Super" |^ "SuperKey"

    val superColumn:SuperColumn[String] =
        "Super" |^ "SuperKey" | "SuperColumn"

    val superColumnVal:Column[String,String] =
        "Super" |^ "SuperKey" | "SuperColumn" |("column", "value")
//
//    val superSlicePredicate = "Super" |^ "key" \\("column","column1")
//    val superSlicePredicate2 = "Super" |^ "key" | "SuperColumn" \\("column","column1")
//    val superSliceRange = "Super" |^ "key" \("start","finish",true,10)
//    val superSliceRange2 = "Super" |^ "key" | "SuperColumn" \("start","finish",true,10)

//    "Super-s" |^ "SuperKey" | "SuperColumn" |("column", "value").save
  }
}