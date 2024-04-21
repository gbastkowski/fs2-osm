INSERT INTO railways   (osm_id, name, official_name, operator, geom,                 tags)
SELECT                  osm_id, name, official_name, operator, ST_MakePolygon(geom), tags
FROM (
    SELECT              ways.osm_id                                     AS osm_id,
                        ways.name                                       AS name,
                        ways.tags->>'official_name'                     AS official_name,
                        ways.tags->>'operator'                          AS operator,
                        ST_MakeLine(array_agg(nodes.geom)::geometry[])  AS geom,
                        ways.tags                                       AS tags
    FROM                ways
    CROSS JOIN LATERAL  unnest(ways.nodes)                              AS node_id
    INNER JOIN          nodes                                           ON nodes.osm_id = node_id
    WHERE               ways.tags->>'landuse' = 'railway'
    GROUP BY            ways.osm_id
) AS grouped_nodes
WHERE ST_IsClosed(geom)
