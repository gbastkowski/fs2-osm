package fs2.osm
package postgres

import cats.effect.Async
import doobie.util.transactor.Transactor

type Transformer[F[_]] = (Summary, Transactor[F]) => Summary
