INSERT INTO osm_lines
SELECT osm_id,
       name,
       tags,
       ST_MakeLine(array_agg(geom)) AS geom
FROM (
    SELECT  ways.osm_id,
            ways.name,
            ways.tags,
            ways_nodes.index,
            nodes.geom::geometry
    FROM    ways
    INNER JOIN ways_nodes ON ways.osm_id          = ways_nodes.way_id
    INNER JOIN nodes      ON ways_nodes.node_id   = nodes.osm_id
    ORDER BY   index
) AS nodes
GROUP BY osm_id, name, tags
