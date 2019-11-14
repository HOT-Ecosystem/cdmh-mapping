# CDMH Mapping Data

Scripts that explore CDMH mapping data, and import data into neo4j database. 

## Getting Started

Extract the CDMH data into data directory. Use the Jupyter notebook "explore.ipynb" to explore 
the data. 

Using the Neo4J "LOAD CSV" import tool is perferred since it's fast. First copy the CDMH csv files to 
`/neo4j_home/import/cdmh`. Then, from command line, 

```
cypher-shell -u username -p password --format plain < cdmh-import.cql
```

I also wrote a python script `import.py` to import data into neo4j. It's only kept here for reference. Note: this process can take over 10 hours. 

```
python import.py bolt://host:port username password
```





