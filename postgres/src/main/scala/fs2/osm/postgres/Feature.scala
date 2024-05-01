package fs2.osm
package postgres

import cats.*
import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.free.ConnectionIO
import doobie.free.connection.ConnectionOp
import doobie.util.fragment.Fragment
import java.net.URL
import org.apache.logging.log4j.scala.Logging
import scala.io.Source
import doobie.util.transactor.Transactor

trait Feature extends Logging {
  def run[F[_]: Async](xa: Transactor[F]): List[F[(String, Int)]] =
    dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } }

  def tableDefinitions: List[Table]
  def dataGenerator:    List[(String, ConnectionIO[Int])]

  protected def logAndRun(resource: URL): ConnectionIO[scala.Int] = logAndRun(Source.fromURL(resource).mkString)

  protected def logAndRun(sql: String): ConnectionIO[scala.Int] = {
    logger.debug(sql)
    Fragment.const(sql).update.run
  }

  protected def logAndRun(sql: Fragment): ConnectionIO[scala.Int] = {
    logger.debug(sql.update.sql)
    sql.update.run
  }
}
