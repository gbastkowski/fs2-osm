package fs2.osm

import cats.effect.*
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.{Files, Path}
import weaver.*
import org.openstreetmap.osmosis.osmbinary.fileformat.Blob
import com.google.protobuf.ByteString

object BlobSpec extends SimpleIOSuite {
  pureTest("parses OSMData compressed") {
    val blob = Blob.of(
      raw               = Option.empty,
      rawSize           = 126972.some,
      zlibData          = ByteString.readFrom(getClass.getResourceAsStream("/data.blob")).some,
      lzmaData          = Option.empty,
      oBSOLETEBzip2Data = Option.empty
    )

    def primitiveBlock = blob.toPrimitiveBlock

    def groups = primitiveBlock.get.primitivegroup
    def size = groups.size

    expect.all(
      primitiveBlock.isSuccess,
      size == 1,
      groups.head.dense.size == 1,
    )
  }

  // test("write file") {
  //   val bytes = PbfReader
  //     .stream(Files[IO].readAll(Path("/Users/gunnar.bastkowski/Downloads/bremen-latest.osm.pbf")))
  //     .filter(_._1.`type` == "OSMData")
  //     .take(1)
  //     .map(_._2)
  //     .map(_.zlibData.get.toByteArray)
  //     .flatMap(byteArray => Stream.emits(byteArray.toSeq))

  //   for {
  //     list <- bytes.compile.toList
  //     count = list.size
  //     written <- bytes.through(Files[IO].writeAll(Path("data.blob"))).compile.count
  //   } yield expect(count == 74870)
  // }
}
