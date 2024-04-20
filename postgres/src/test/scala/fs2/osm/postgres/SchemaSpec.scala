package fs2.osm
package postgres

import cats.Show
import cats.effect.*
import cats.syntax.all.*
import doobie.implicits.*
import org.scalacheck.Gen
import weaver.*
import weaver.scalacheck.*

object SchemaSpec extends SimpleIOSuite with Checkers {
  import Schema.*

  test("CREATE TABLE") {
    forall(genTable) { table =>
      val tableDefinition = (
        table.columns    .map { _.sqlDefinition } :::
        table.constraints.map { _.sqlDefinition }
      ).mkString(", ")

      expect.eql(
        s"CREATE TABLE IF NOT EXISTS ${table.name} ( $tableDefinition )",
        table.create,
      )
    }
  }

  test("column definitions") {
    forall(genColumn) { column =>
      val expectedDataType = column.datatype match {
        case VarChar               => "VARCHAR"
        case BigInt                => "BIGINT"
        case BigIntArray           => "BIGINT[]"
        case Geography(tpe, srid)  => s"GEOGRAPHY($tpe, $srid)"
        case Jsonb                 => "JSONB"
      }

      val expectedConstraint = column.constraint match {
        case Some(PrimaryKey)      => "PRIMARY KEY"
        case Some(NotNull)         => "NOT NULL"
        case None                  => ""
      }

      expect.eql(
        s"${column.name} $expectedDataType $expectedConstraint".trim,
        column.sqlDefinition,
      )
    }
  }

  private given Show[Column] = _.toString
  private given Show[Table] = _.toString

  private lazy val genColumn =
    for
      name         <- Gen.alphaStr
      datatype     <- Gen.oneOf[Datatype](BigInt, BigIntArray, Jsonb, VarChar)
      constraint   <- Gen.option[ColumnConstraint](Gen.oneOf(NotNull, PrimaryKey))
    yield Column(name, datatype, constraint)

  private lazy val genTable =
    for
      name         <- Gen.alphaStr
      columns      <- Gen.nonEmptyListOf(genColumn)
      constraints  <- Gen.listOf(Gen.oneOf[TableConstraint](
                                   ForeignKeyConstraint("a", "b" -> "c"),
                                   UniqueConstraint("a", "b")))
    yield Table(name, columns, constraints)

  pureTest("a table with a single varchar column") {
    val subject = Schema(
      Table("test", Column("key", VarChar)))

    expect.all(
      subject.create.head == "CREATE TABLE IF NOT EXISTS test ( key VARCHAR )",
      subject.drop.head   == "DROP TABLE IF EXISTS test CASCADE"
    )
  }

  pureTest("a table with multiple columns") {
    val subject = Schema(
      Table("test",
            Column("key", BigInt),
            Column("value", VarChar)))

    expect.all(
      subject.create.head == "CREATE TABLE IF NOT EXISTS test ( key BIGINT, value VARCHAR )",
      subject.drop.head   == "DROP TABLE IF EXISTS test CASCADE"
    )
  }
}
