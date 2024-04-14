package fs2.osm
package core

import cats.syntax.all.*
import com.google.protobuf.ByteString
import java.nio.charset.Charset
import java.time.Instant
import org.openstreetmap.osmosis.osmbinary.osmformat
import org.openstreetmap.osmosis.osmbinary.osmformat.DenseInfo
import org.openstreetmap.osmosis.osmbinary.osmformat.StringTable
import weaver.*

object DenseNodeStreamSpec extends SimpleIOSuite {
  pureTest("id delta encoding") {
    val denseNodes = osmformat.DenseNodes(
      id        = Seq(42, 1, 2),
      lat       = Seq(530000000, 10000000, 2000000),
      lon       = Seq(100000000, 10000000, 2000000),
      keysVals  = Seq.empty,
      denseinfo = Option.empty
    )
    val list = DenseNodeStream(StringTable(Seq.empty)).nodes(denseNodes).compile.toList
    expect.all(
      list(0).osmId == 42L,
      list(1).osmId == 43L,
      list(2).osmId == 45L
    )
  }

  pureTest("default offsets and granularity") {
    val denseNodes  = osmformat.DenseNodes(
      id        = Seq(42, 1, 1),
      lat       = Seq(530000000, 10000000, 2000000),
      lon       = Seq(100000000, 10000000, 2000000),
      keysVals  = Seq.empty,
      denseinfo = Option.empty
    )

    val list = DenseNodeStream(StringTable(Seq.empty)).nodes(denseNodes).compile.toList

    expect.all(
      list(0).coordinate == Coordinate(10.0, 53.0),
      list(1).coordinate == Coordinate(11.0, 54.0),
      list(2).coordinate == Coordinate(11.2, 54.2)
    )
  }

  pureTest("specific offsets and default granularity") {
    val denseNodes  = osmformat.DenseNodes(
      id        = Seq(42, 1, 1),
      lat       = Seq(530000000, 10000000, 2000000),
      lon       = Seq(100000000, 10000000, 2000000),
      keysVals  = Seq.empty,
      denseinfo = Option.empty
    )

    val list = DenseNodeStream(StringTable(Seq.empty), latOffset = 200000000, lonOffset = 100000000)
      .nodes(denseNodes)
      .compile
      .toList

    expect.all(
      list(0).coordinate == Coordinate(10.1, 53.2),
      list(1).coordinate == Coordinate(11.1, 54.2),
      list(2).coordinate == Coordinate(11.3, 54.4)
    )
  }

  pureTest("granularity of one degree") {
    val denseNodes  = osmformat.DenseNodes(
      id        = Seq(42, 1),
      lat       = Seq(53, 1),
      lon       = Seq(10, 1),
      keysVals  = Seq.empty,
      denseinfo = Option.empty
    )

    val list = DenseNodeStream(StringTable(Seq.empty), granularity = 1000000000).nodes(denseNodes).compile.toList

    expect.all(
      list(0).coordinate == Coordinate(10.0, 53.0),
      list(1).coordinate == Coordinate(11.0, 54.0),
    )
  }

  pureTest("streams one without info") {
    val stringTable = osmformat.StringTable(
      Seq(
        ByteString.copyFrom(Array[Byte](0)),
        ByteString.copyFrom("message", `UTF-8`),
        ByteString.copyFrom("hello", `UTF-8`),
        ByteString.copyFrom("receiver", `UTF-8`),
        ByteString.copyFrom("world", `UTF-8`),
        ByteString.copyFrom("user", `UTF-8`)
      )
    )
    val denseNodes  = osmformat.DenseNodes(
      id          = Seq(42, 1),
      lat         = Seq(53, 1),
      lon         = Seq(10, 1),
      keysVals    = Seq(1, 2, 3, 4),
      denseinfo   = DenseInfo(
        version   = Seq(10, 11),
        timestamp = Seq(123, 124),
        changeset = Seq(1, 1),
        uid       = Seq(1, 1),
        userSid   = Seq(5, 5),
        visible   = Seq(true, true)
      ).some
    )

    val list = DenseNodeStream(stringTable, granularity = 1000000000).nodes(denseNodes).compile.toList

    expect.all(
      list.size == 2,
      list(0).osmId == 42,
      list(0).tags == Map("message" -> "hello", "receiver" -> "world"),
      list(0).info.version == 10.some,
      list(0).info.timestamp == Instant.ofEpochSecond(123).some,
      list(0).info.changeset == 1L.some,
      list(0).info.userId == 1.some,
      list(0).info.userName == "user".some,
      list(0).info.visible == true.some,

      list(1).osmId == 43,
      list(1).tags == Map("message" -> "hello", "receiver" -> "world"),
      list(1).info.version == 11.some,
      list(1).info.timestamp == Instant.ofEpochSecond(124).some,
      list(1).info.changeset == 1L.some,
      list(1).info.userId == 1.some,
      list(1).info.userName == "user".some,
      list(1).info.visible == true.some,
    )
  }
}
