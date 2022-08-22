# Parse Crawl
A file that takes a .wat file as input and outputs a sites.csv file for nodes, and a links.csv file for relationships.

The csv files can then be imported to Neo4j.

I tried using tools like PySpark and Cython, but they had little to no improvement on efficiency.

# Benchmarks for sample file (approx)
Input file: 1.44 GB

Output file (total): 547 MB

Runtime (total): 52 seconds

# Estimated benchmarks with full file (June/July archive)
Input file: 21 TB

Output files (total): 9 TB

Runtime (total): 10 days
