package fs2.osm.postgres

import cats.*
import cats.data.Kleisli
import cats.effect.*
import cats.syntax.all.*
import fs2.Pure
import fs2.{Pipe, Pull, Stream}
import org.apache.logging.log4j.scala.Logging
import weaver.*

object StreamOperationsSpec extends SimpleIOSuite with Logging {
  case class Sum(birds: Int = 0, cats: Int = 0, dogs: Int = 0)

  sealed trait Pet
  case class Bird(name: String) extends Pet
  case class Cat(name: String) extends Pet
  case class Dog(name: String) extends Pet

  val pets: Stream[IO, Pet] = Stream
    .emits(
      List(
        Bird("bird a"), Bird("bird b"), Bird("bird c"),
        Cat("cat a"), Cat("cat b"), Cat("cat c"),
        Dog("dog a"), Dog("dog b"), Dog("dog c")))
    .debug(_.toString, logger.debug)

  val birdPipe: Pipe[IO, Pet, Sum] = b => b
    .collect { case b: Bird => b }
    .chunkN(2, allowFewer = true)
    .fold(Sum()) { (sum, chunk) => sum.copy(birds = sum.birds + chunk.size) }

  val catPipe: Pipe[IO, Pet, Sum] = b => b
    .collect { case c: Cat => c }
    .chunkN(2, allowFewer = true)
    .fold(Sum()) { (sum, chunk) => sum.copy(cats = sum.cats + chunk.size) }

  val dogPipe: Pipe[IO, Pet, Sum] = b => b
    .collect { case d: Dog => d }
    .chunkN(2, allowFewer = true)
    .fold(Sum()) { (sum, chunk) => sum.copy(dogs = sum.dogs + chunk.size) }

  test("broadcast") {
    val n = pets.broadcastThrough(birdPipe, catPipe, dogPipe)

    for
      sums <- n.compile.toList
      sum   = sums.fold(Sum()) { (a, b) => Sum(birds = a.birds + b.birds, cats = a.cats + b.cats, dogs = a.dogs + b.dogs) }
    yield
      expect.all(
        sums.size == 3,
        sum == Sum(birds = 3, cats = 3, dogs = 3)
      )
  }
}
