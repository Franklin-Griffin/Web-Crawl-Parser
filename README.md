# Parse Crawl
A file that takes a .wat file as input and outputs a sites.csv file for nodes, and a links.csv file for relationships.

The csv files can then be imported to Neo4j.

# How to Merge CSV Files
In command prompt, navigate to the directory storing the CSV files

Run `copy *.csv combined-csv-files.csv`

# How to Send to Neo4j
Copy/move the folders "nodes" and "rels" from OUTPUT into /import

Open terminal of database

`cd bin`

`neo4j-admin import --nodes=SITE=import/nodes/.*.csv --relationships=LINKS_TO=import/rels/.*.csv --auto-skip-subsequent-headers --force`