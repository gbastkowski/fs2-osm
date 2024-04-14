package fs2.osm

import com.google.protobuf.ByteString
import java.io.{ByteArrayInputStream, DataInputStream}
import java.nio.charset.Charset
import java.util.zip.Inflater
import org.openstreetmap.osmosis.osmbinary.fileformat.{Blob, BlobHeader}
import org.openstreetmap.osmosis.osmbinary.osmformat.{PrimitiveBlock, StringTable}
import scala.util.Try

package object core {

  extension (stringTable: StringTable) {
    def getString(index: Int): String = stringTable.s(index).toString(`UTF-8`)

    def tags(keys: Seq[Int], values: Seq[Int]): Map[String, String] =
      (keys, values).zipped
        .map { (key, value) => getString(key) -> getString(value) }
        .toMap

    def tags(keyValueSequence: Iterator[Int]): Map[String, String] =
      keyValueSequence
        .grouped(2)
        .map { case Seq(head, last) => getString(head) -> getString(last) }
        .toMap
  }

  extension (blob: Blob) {
    def toPrimitiveBlock: Try[PrimitiveBlock] = Try {
      val byteArray = (blob.raw, blob.zlibData, blob.rawSize) match {
        case (Some(raw), _, _)                      => raw.toByteArray
        case (None, Some(zlibData), Some(rawSize))  => inflate(zlibData, rawSize)
        case _ => throw new IllegalArgumentException(s"Blob of didn't have raw or zlibData")
      }
      PrimitiveBlock.parseFrom(DataInputStream(ByteArrayInputStream(byteArray)))
    }

    private def inflate(bytes: ByteString, rawSize: Int) = {
      val inflater = new Inflater()
      val buffer = new Array[Byte](rawSize)
      inflater.setInput(bytes.toByteArray)
      inflater.inflate(buffer)
      inflater.end()
      buffer
    }
  }

  val `UTF-8`: Charset = Charset.forName("UTF-8")
}
