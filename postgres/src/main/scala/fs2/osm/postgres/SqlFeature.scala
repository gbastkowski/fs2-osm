package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import fs2.Stream
import java.net.URL
import org.apache.logging.log4j.scala.Logging
import scala.io.Source
import cats.Foldable
import scala.collection.immutable.StrictOptimizedSeqOps

trait SqlFeature extends Feature {
  def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  def dataGenerator:    List[(String, ConnectionIO[Int])]

  protected def logAndRun(resource: URL): ConnectionIO[Int] = logAndRun(Source.fromURL(resource).mkString)
  protected def logAndRun(sql: String):   ConnectionIO[Int] = logAndRun(Fragment.const(sql))
  protected def logAndRun(sql: Fragment): ConnectionIO[Int] = {
    logger.debug(sql.update.sql)
    sql.update.run
  }
}
