from orjson import loads # 6x faster than built-in
from time import time
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.functions import explode

# constants
STARTING_ROW, JUMP, = 44, 32

# configs
partitions, output_path = 1250, "file:///C:/Users/frank/Documents/GitHub/ParseCrawl/OUTPUT"

def main():
    sc = SparkSession\
    .builder\
    .master("local")\
    .config("spark.executor.memory", "2g")\
    .config("spark.driver.memory", "2g")\
    .config("spark.executor.memoryOverhead", "2g")\
    .config("spark.driver.maxResultSize", "2g")\
    .appName("PLACEHOLDER")\
    .getOrCreate()\
    .sparkContext
    sqlc = SQLContext(sc)
    
    # don't measure start up, it is constant
    start = time()

    rdd1 = sc.parallelize(open_wat(), numSlices=partitions)
    rdd2 = rdd1.mapPartitionsWithIndex(read_record)
    siteSchema = StructType([
                    StructField(":ID", StringType()), # partition num + "\" + id in partition (ie "123 49")
                    StructField("url", StringType()),
                    StructField("title", StringType())
                 ])
    linkSchema = StructType([
                    StructField(":START_ID", StringType()), # same IDs used
                    StructField(":END_ID", StringType())
                 ])
    schema = StructType([
                StructField("sites", ArrayType(siteSchema)),
                StructField("links", ArrayType(linkSchema))
             ])
    df = sqlc.createDataFrame(rdd2, schema)
    df.createOrReplaceTempView("df")
    siteData = df.select(explode(df.sites))
    expandedSiteData = siteData.select("col.*")
    expandedSiteData.write.option("header", True).option("escape", '"').csv(output_path + "/nodes")
    linkData = df.select(explode(df.links))
    expandedLinkData = linkData.select("col.*")
    expandedLinkData.write.option("header", True).option("escape", '"').csv(output_path + "/rels")

    print(time() - start)


# takes in a list of string JSONs of data
# returns lists in format: 
# [[[:ID, title], [:ID, title]...], [[:START_ID, :END_ID], [:START_ID, :END_ID]...]]]
def read_record(splitIndex, records):
    pid = "!" # the id of the partition
    for i in range(splitIndex):
        # setting pid
        pid = increment(pid)
    id = "!" # the counting id for all elements in partition
    for record in records:
        swrite = []
        lwrite = []
        data = loads(record)['Envelope']
        url = repr(data['WARC-Header-Metadata']['WARC-Target-URI'])[1:-1]
        curid = " ".join([pid, id]) # the id of the site being linked to
        rootAppended = False

        try:
            title = repr(data['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Title'])[1:-1]
            swrite.append([curid, url, title])
            rootAppended = True
            id = increment(id)

            linkbook = data['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
            for link in linkbook:
                l = "" # the link
                if "url" in link:
                    l = link["url"]
                elif link:
                    # links always include "href" or "url" unless they are empty
                    l = link["href"]
                l = repr(l)[1:-1]
                    
                t = "" # the title
                if "title" in link:
                    # can be either
                    t = link["title"]
                elif "text" in link:
                    # the text of a link to a site
                    t = link["text"]
                elif "alt" in link:
                    # the alt of a link to an image
                    t = link["alt"]
                t = repr(t)[1:-1]
                    
                if len(l) >= 2 and l[0] == "/" and l[1] != "/":
                    # two slashes seems to be an API connection
                    # but one slash is a directory
                    newUrl = "".join([url if url[-1] != "/" else url[:-1], "/", l[1:]])
                    swrite.append([" ".join([pid, id]), newUrl, t])
                    # must be included, just in case (think stack overflow)
                    lwrite.append([" ".join([pid, id]), curid])
                    id = increment(id)
                elif len(l) >= 8 and (l[:7] == "http://" or l[:8] == "https://"):
                    # this is a link to a site or image
                    # we need to make sure it gets included also
                    swrite.append([" ".join([pid, id]), l, t])
                    lwrite.append([" ".join([pid, id]), curid])
                    id = increment(id)
                # Anything else is somehting like javascript or php,
                # which is not accessed by a search engine

        except:
            # site does not have HTML Metadata (no title and/or no links)
            if not rootAppended:
                # prevent writing the root (home page) twice
                swrite.append([curid, url, ""])
                id = increment(id)

        yield [swrite, lwrite]


# creates a generator of JSON string records inside the wat
def open_wat():
    with open("TestData.wat", encoding="utf-8") as f:
        # set buffer, skip header
        for i in range(STARTING_ROW):
            next(f)

        try:
            while True:
                yield f.readline()
                for i in range(JUMP):
                    next(f)
        except Exception as e:
            # end of file
            pass


# ASCII ID for smaller filesize, from 33-126 (because of whitespace trimming), 
# skipping 22, 34, and 92, aka comma, quote, and backslash (ex comma, between '-' and '+' on ASCII chart)
# uses much less memory with almost no added time
def increment(id):
    for i in range(len(id) - 1, -1, -1):
        x = id[i]
        if x != "~":
            return "".join([id[:i], ("-" if x == "+" else ("#" if x == "!" else ("]" if x == "[" else chr(ord(x) + 1)))), id[i+1:]])
        id = "".join([id[:i], "!", id[i+1:]])
    # new character
    return "".join(["!", id])


if __name__ == "__main__":
    main()