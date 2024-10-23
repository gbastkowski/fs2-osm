package fs2.osm
package postgres

import cats.effect.Async
import doobie.util.transactor.Transactor
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*

case class Config(jdbcUrl: String, username: String, password: String) derives ConfigReader {
  def transactor[F[_]: Async]: Transactor[F] =
      Transactor.fromDriverManager[F](
        // driver      = "org.postgresql.Driver",
        driver      = "net.postgis.jdbc.jts.JtsGisWrapper",
        url         = jdbcUrl,
        user        = username,
        password    = password,
        logHandler  = None
      )
}
