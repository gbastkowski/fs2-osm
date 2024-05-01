package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import fs2.Chunk
import java.net.URL
import org.apache.logging.log4j.scala.Logging
import scala.io.Source
import cats.Foldable

trait Importer extends Logging {
  def run[F[_]: Async](xa: Transactor[F]): List[F[(String, Int)]] =
    dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } }

  def tableDefinitions: List[Table]
  def dataGenerator:    List[(String, ConnectionIO[Int])]

  // protected def logAndRun(resource: URL): ConnectionIO[Int] = logAndRun(Source.fromURL(resource).mkString)
  // protected def logAndRun(sql: String):   ConnectionIO[Int] = logAndRun(Fragment.const(sql))
  protected def logAndRun[F[_]: Async, T: Write](sql: Fragment, tuples: Chunk[T]): ConnectionIO[Int] = {
    logger.debug(sql.update.sql)
    Update[T](sql.update.sql).updateMany(tuples)
    sql.update.run
  }
}
