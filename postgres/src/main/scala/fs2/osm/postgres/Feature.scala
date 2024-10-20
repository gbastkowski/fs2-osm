package fs2.osm
package postgres

import cats.effect.Async
import doobie.Transactor
import fs2.Stream
import org.apache.logging.log4j.scala.Logging

trait Feature extends Logging {
  def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)]
  def tableDefinitions: List[Table]
}
