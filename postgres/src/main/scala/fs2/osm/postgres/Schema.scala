package fs2.osm
package postgres

import cats.syntax.all.*
import doobie.implicits.*
import doobie.util.fragment.Fragment
import doobie.util.update.Update0

object Schema {

  extension (schema: Schema) {
    def create: Seq[String] = schema.tables.map { _.create }
    def drop:   Seq[String] = schema.tables.map { _.drop }
  }

  extension (table: Table) {
    def create: String = createTable(table.name, table.columns.map(_.sqlDefinition))
    def drop:   String = s"DROP TABLE IF EXISTS ${table.name} CASCADE"
  }

  extension (column: Column) {
    def sqlDefinition: String = Seq(
      column.name,
      column.datatype match {
        case g: Geography  => "geography(Point, 4326)"
        case BigInt        => "bigint"
        case Jsonb         => "jsonb"
        case VarChar       => "varchar"
      }
    ).mkString(" ")
  }

  private def createTable(name: String, columns: Seq[String]) =
    s"CREATE TABLE IF NOT EXISTS ${name} ( ${columns.mkString(", ")} )"
}

case class Schema(tables: Table*)

case class Table(name: String, columns: Column*)

case class Column(name: String, datatype: Datatype)

sealed trait Datatype
case class  Geography(t: GeographyType, n: SomeNumber)  extends Datatype
case object BigInt                                      extends Datatype
case object Jsonb                                       extends Datatype
case object VarChar                                     extends Datatype

sealed trait GeographyType
case object Point                                       extends GeographyType

sealed trait SomeNumber
case object `4326`                                      extends SomeNumber
