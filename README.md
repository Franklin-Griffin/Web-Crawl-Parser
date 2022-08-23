# Parse Crawl
A file that takes a .wat file as input and outputs a sites.csv file for nodes, and a links.csv file for relationships.

The csv files can then be imported to Neo4j.

Cython decreases runtime by 7% (49->45.75)

# How to Run w/Cython
Install requirements with pip

Start python with `python`

Run with `import parse`

# Benchmarks for sample file (approx)
Input file: 1.44 GB

Output file (total): 547 MB

Runtime (total, on my laptop): 45.75 seconds

If you would like to download the file and see how it compares to my laptop's speed, it can be found at https://data.commoncrawl.org/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wat/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wat.gz

# Estimated benchmarks with full file (June/July archive)
Input file: 21 TB

Output files (total): 9 TB

Runtime (total, if run on my laptop): 8 days
