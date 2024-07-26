package fs2.osm
package core

import fs2.Stream
import org.openstreetmap.osmosis.osmbinary.osmformat.{PrimitiveGroup, StringTable}

private[core] def EntityStream[F[_]](group: PrimitiveGroup, stringTable: StringTable): Stream[F, OsmEntity] =
  group match
    case _ if group.relations.nonEmpty   => Stream.emits(group.relations.map { relation => Relation(stringTable, relation) })
    case _ if group.ways.nonEmpty        => Stream.emits(group.ways     .map { way      => Way(stringTable, way) })
    case _ if group.dense.nonEmpty       => group.dense.fold(Stream.empty)   { DenseNodeStream(stringTable).nodes }
    case _ if group.changesets.nonEmpty  => throw new NotImplementedError("ChangeSets not implemented yet.")
    case _ if group.nodes.nonEmpty       => throw new NotImplementedError("Nodes not implemented yet.")
    case _                               => throw new Exception("Unknown primitive group found")
