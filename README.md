# Parse Crawl
A file that takes a .wat file as input and outputs a sites.csv file for nodes, and a links.csv file for relationships.

The csv files can then be imported to Neo4j.

Cython decreases runtime by 7% (49->45.75)

# How to Run w/Cython
Install requirements with pip

`python setup.py build_ext --inplace` to build, but this repo already has a build

Start python with `python`

Run with `import parse`

# How to Send to Neo4j
Upload csv files to /import

Open terminal of database

`cd bin`

`neo4j-admin import --nodes=SITE=import/sites.csv --relationships=LINKS_TO=import/links.csv --multiline-fields --force`

# Benchmarks for sample file (approx)
Input file: 1.44 GB

Output file (total): 547 MB

Runtime (total, including Neo4j send, on my laptop): 60.75 seconds

If you would like to download the file and see how it compares to my laptop's speed, it can be found at https://data.commoncrawl.org/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wat/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wat.gz

# Estimated benchmarks with full file (June/July archive)
Input file: 21 TB

Output files (total): 9 TB

Runtime (total, if run on my laptop): 10.5 days