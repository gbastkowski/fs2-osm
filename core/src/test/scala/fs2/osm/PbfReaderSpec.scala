package fs2.osm

import cats.effect.*
import cats.syntax.all.*
import fs2.*
import fs2.io.file.{Files, Path}
import fs2.io.readInputStream
import weaver.*

object PbfReaderSpec extends SimpleIOSuite {
  private val bremen = {
    val inputStream = IO(getClass.getResourceAsStream("/bremen-20240402.osm.pbf"))
    readInputStream(inputStream, 1024, closeAfterUse = true)
  }

  test("correct number of blobs is available") {
    for   count <- PbfReader.stream(bremen).compile.count
    yield expect(count == 244)
  }

  test("blob and header have correct values") {
    for
      count  <- PbfReader.stream(bremen).compile.count
      list   <- PbfReader.stream(bremen).take(3).compile.toList
    yield expect.all(
      count == 244,
      list(0)._1.`type` == "OSMHeader",
      list(0)._1.datasize == 180,

      list(1)._1.`type` == "OSMData",
      list(1)._1.datasize == 74878,
      list(1)._2.raw.isEmpty,
      list(1)._2.rawSize.get == 126972,
      list(1)._2.zlibData.nonEmpty,
      list(1)._2.toPrimitiveBlock.isSuccess
    )
  }
}
