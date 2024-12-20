package fs2.osm
package postgres

import cats.effect.Async
import doobie.Transactor
import fs2.Stream
import org.apache.logging.log4j.scala.Logging

/**
 * A marker trait to make sure that only optional traits can be added to the exporter.
 */
trait OptionalFeature extends Feature

trait Feature extends Logging {
  def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)]
  def tableDefinitions: List[Table]
}
