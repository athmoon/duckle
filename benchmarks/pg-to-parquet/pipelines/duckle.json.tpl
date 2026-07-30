{
  "name": "pg-to-parquet",
  "nodes": [
    {"id": "pg", "type": "source", "position": {"x": 0, "y": 0},
     "data": {"label": "Postgres", "componentId": "src.postgres",
       "properties": {"host": "__HOST__", "port": __PORT__, "database": "__DB__",
                      "user": "__USER__", "password": "__PASS__", "tableName": "__TABLE__"}}},
    {"id": "pq", "type": "sink", "position": {"x": 240, "y": 0},
     "data": {"label": "Parquet", "componentId": "snk.parquet",
       "properties": {"path": "__OUT__"}}}
  ],
  "edges": [
    {"id": "e1", "source": "pg", "target": "pq", "sourceHandle": "main",
     "targetHandle": "main", "data": {"connectionType": "main"}}
  ]
}
