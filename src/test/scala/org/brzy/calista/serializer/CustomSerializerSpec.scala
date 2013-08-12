package org.brzy.calista.serializer

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.WordSpec
import java.nio.ByteBuffer
import org.brzy.calista.schema.StandardFamily

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
class CustomSerializerSpec extends WordSpec with ShouldMatchers {

  class Demo(val name:String) extends Serializable

  "Serializers" should {
    "allow addition of custom serializers" in {
      val myCustomSerializer = new Serializer[Demo]() {
        val typeManifest = manifest[Demo]
        def toBytes(t: Demo) = ByteBuffer.wrap(Array.empty[Byte])
        def fromBytes(b: ByteBuffer) = new Demo("")
      }
//      implicit val serializers = Serializers.add(Seq(myCustomSerializer))
//      StandardFamily("Serializer")("id")("data").set(new Demo("hello"))
    }
  }
}
