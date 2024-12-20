package fs2.osm

import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.circe.json.implicits.*
import io.circe.Json
import io.circe.syntax.*
import net.postgis.jdbc.geometry.LineString

package object postgres {

  extension (tags: Map[String, String]) def toJson: Json = Json.obj(tags.mapValues { _.asJson }.toSeq*)

  // TODO get rid of
  extension (a: LineString) def merge(b: LineString): Option[LineString] =
    if      (a.getLastPoint  == b.getFirstPoint) a.concat(b).some
    else if (a.getFirstPoint == b.getLastPoint)  b.concat(a).some
    else if (a.getFirstPoint == b.getFirstPoint) a.reverse.concat(b).some
    else if (a.getLastPoint  == b.getLastPoint)  a.concat(b.reverse).some
    else Option.empty


  given Get[Map[String, String]] = Get[Json].map { json =>
    json.asObject
      .map { _.toMap.flatMap { (k, v) => v.asString.map { k -> _ } } }
      .getOrElse(Map.empty)
  }
  given Put[Map[String, String]] = Put[Json].contramap(_.asJson)

}
