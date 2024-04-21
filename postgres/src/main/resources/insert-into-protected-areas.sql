INSERT INTO protected_areas (osm_id, name, kind, geom, tags)
SELECT                  osm_id, name, kind, ST_MakePolygon(geom), tags
FROM (
    SELECT              ways.osm_id                                     AS osm_id,
                        ways.name                                       AS name,
                        ways.tags->>'protection_title'                  AS kind,
                        ST_MakeLine(array_agg(nodes.geom)::geometry[])  AS geom,
                        ways.tags                                       AS tags
    FROM                ways
    CROSS JOIN LATERAL  unnest(ways.nodes)                              AS node_id
    INNER JOIN          nodes                                           ON nodes.osm_id = node_id
    WHERE               ways.tags->>'boundary' = 'protected_area'
    GROUP BY            ways.osm_id
) AS grouped_nodes
WHERE ST_IsClosed(geom)
