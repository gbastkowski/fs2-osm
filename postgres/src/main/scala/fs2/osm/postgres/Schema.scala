package fs2.osm
package postgres

import cats.syntax.all.*
import doobie.implicits.*
import doobie.util.fragment.Fragment
import doobie.util.update.Update0

object Schema {
  extension (s: Schema)
    def create: Seq[String] = s.tables.map { _.create }
    def drop:   Seq[String] = s.tables.map { _.drop }

  extension (t: Table)
    def create: String = {
      val tableDefinition = (
        t.columns    .map { _.sqlDefinition } :::
        t.constraints.map { _.sqlDefinition }
      )
        .mkString(", ")

      s"CREATE TABLE IF NOT EXISTS ${t.name} ( $tableDefinition )"
    }

    def drop: String = s"DROP TABLE IF EXISTS ${t.name} CASCADE"

  extension (c: Column)
    def sqlDefinition: String =
      List(
        c.name,
        c.datatype match {
          case VarChar               => "VARCHAR"
          case BigInt                => "BIGINT"
          case BigIntArray           => "BIGINT[]"
          case Geography(tpe, srid)  => s"GEOGRAPHY($tpe, $srid)"
          case Int                   => "INTEGER"
          case Jsonb                 => "JSONB"
        },
        c.constraint.map {
          case NotNull(Some(default)) => s"NOT NULL DEFAULT $default"
          case NotNull(_) => "NOT NULL"
          case PrimaryKey => "PRIMARY KEY"
        }.getOrElse("")
      )
        .filter { _.nonEmpty }
        .mkString(" ")

  extension (c: TableConstraint)
    def sqlDefinition: String = c match {
      case c: UniqueConstraint                              => s"UNIQUE (${c.columns.mkString(",")})"
      case ForeignKeyConstraint(column, (table, reference)) => s"FOREIGN KEY ($column) REFERENCES $table($reference)"
    }
}

case class Schema(tables: Table*)

object Table {
  def apply(name: String, columns: Column*): Table = Table(name, columns.toList, List.empty)
}
case class Table(name: String, columns: List[Column], constraints: List[TableConstraint]) {
  def withColumns(columns: Column*):                  Table = copy(columns = columns.toList)
  def withConstraints(constraints: TableConstraint*): Table = copy(constraints = constraints.toList)
}

object Column {
  def apply(name: String, datatype: Datatype): Column = Column(name, datatype, Option.empty)
  def apply(name: String, datatype: Datatype, constraint: ColumnConstraint): Column =
    Column(name, datatype, constraint.some)
}
case class Column(name: String, datatype: Datatype, constraint: Option[ColumnConstraint])

sealed trait TableConstraint
case class UniqueConstraint(columns: String*) extends TableConstraint
case class ForeignKeyConstraint(column: String, references: (String, String)) extends TableConstraint

sealed trait ColumnConstraint
case object PrimaryKey extends ColumnConstraint

object NotNull {
  def apply(): NotNull = NotNull(Option.empty)
  def apply(default: String): NotNull = NotNull(default.some)
}
case class NotNull(default: Option[String]) extends ColumnConstraint

sealed trait Datatype
case class  Geography(t: GeographyType, srid: Srid) extends Datatype
case object Int                                     extends Datatype
case object BigInt                                  extends Datatype
case object BigIntArray                             extends Datatype
case object Jsonb                                   extends Datatype
case object VarChar                                 extends Datatype

sealed trait GeographyType
case object LineString                              extends GeographyType
case object Point                                   extends GeographyType
case object Polygon                                 extends GeographyType

type Srid = Int

val Wgs84: Srid = 4326
