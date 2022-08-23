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

# Estimated benchmarks with full file (June/July archive)
Input file: 21 TB

Output files (total): 9 TB

Runtime (total, if on my laptop): 8 days