INSERT INTO osm_lines
SELECT      ways.osm_id,
            ways.name,
            ways.tags,
            ST_MakeLine(nodes.geom::geometry ORDER BY ways_nodes.index) AS geom
FROM        ways
INNER JOIN  ways_nodes  ON ways.osm_id          = ways_nodes.way_id
INNER JOIN  nodes       ON ways_nodes.node_id   = nodes.osm_id
GROUP BY    ways.osm_id
