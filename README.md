# Parse Crawl
A file that takes a .wat file as input and outputs a sites.csv file for nodes, and a links.csv file for relationships.

The csv files can then be imported to Neo4j.

# How to Send to Neo4j
Upload csv files to /import

Open terminal of database

`cd bin`

`neo4j-admin import --nodes=SITE=import/sites.csv --relationships=LINKS_TO=import/links.csv --force`