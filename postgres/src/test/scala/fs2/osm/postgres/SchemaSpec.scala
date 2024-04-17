package fs2.osm
package postgres

import doobie.implicits.*
import weaver.*

object SchemaSpec extends FunSuite {
  import Schema.*

  test("a table with a single varchar column") {
    val subject = Schema(
      Table("test",
            Column("key", VarChar)))

    expect.all(
      subject.create.head == "CREATE TABLE IF NOT EXISTS test ( key varchar )",
      subject.drop.head   == "DROP TABLE IF EXISTS test CASCADE"
    )
  }

  test("a table with multiple columns") {
    val subject = Schema(
      Table("test",
            Column("key", BigInt),
            Column("value", VarChar)))

    expect.all(
      subject.create.head == "CREATE TABLE IF NOT EXISTS test ( key bigint, value varchar )",
      subject.drop.head   == "DROP TABLE IF EXISTS test CASCADE"
    )
  }
}
