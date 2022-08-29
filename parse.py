from orjson import loads # 6x faster than built-in
from time import time

STARTING_ROW, JUMP = 44, 32

def main():
    sites = open("sites.csv", "w", encoding="utf-8")
    links = open("links.csv", "w", encoding="utf-8")

    # we don't want to measure startup time, it's constant
    start = time()

    # headers

    sites.write(":ID,title\n")
    # the URL is the ID (much faster in this context, but at the cost of a little storage)
    # if a title is not known, it will be an empty string
    # if site is a link, the link's text/alt will be used as a title
    # if one is not found, an empty string will be given
    # in the search engine, the url (aka ID) can be used, but doing that here takes much more space

    links.write(":START_ID,:END_ID\n")
    # written in URLs

    records = open_wat()

    for record in records:
        pair = read_record(record)
        sites.writelines(pair[0])
        links.writelines(pair[1])

    print(time() - start)

    sites.close()
    links.close()


# takes in a string JSON of data
# returns a tuple in format: (what to write to sites.csv, what for links.csv)
def read_record(record):
    swrite = []
    lwrite = []
    data = loads(record)['Envelope']
    url = q(data['WARC-Header-Metadata']['WARC-Target-URI'])
    try:
        title = q(data['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Title'])
        swrite.append(",".join([url, title]) + "\n")

        linkbook = data['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
        for link in linkbook:
            l = "" # the link
            if "url" in link:
                l = link["url"]
            elif link:
                # links always include "href" or "url" unless they are empty
                l = link["href"]
                
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
                
            if len(l) >= 2 and l[0] == "/" and l[1] != "/":
                # two slashes seems to be an API connection
                # but one slash is a directory
                newUrl = "".join([url[:-1] if url[-2] != "/" else url[:-2], q(l)[1:]])
                swrite.append(",".join([newUrl, q(t)]) + "\n")
                # also must be included, just in case (think stack overflow)
                lwrite.append(",".join([url, newUrl]) + "\n")
            elif len(l) >= 8 and (l[:7] == "http://" or l[:8] == "https://"):
                # this is a link to a site or image
                # we need to make sure it gets included
                # if it is a duplicate, that's ok, it'll get filtered
                swrite.append(",".join([q(l), q(t)]) + "\n")
                lwrite.append(",".join([url, q(l)]) + "\n")
            # Anything else is somehting like javascript or php,
            # which is not accessed by a search engine

    except:
        # site does not have HTML Metadata (no title, no links)
        swrite.append(url + ",\n")

    return (swrite, lwrite)


def open_wat():
    with open("TestData.wat", encoding="utf-8") as f:
        # set buffer, skip header
        for i in range(STARTING_ROW):
            next(f)

        # create a generator of JSON records
        try:
            while True:
                yield f.readline()
                for i in range(JUMP):
                    next(f)
        except Exception as e:
            # end of file
            pass


# wrap in quotes and protect quotes and newlines, shorthand
def q(s):
    return "".join(['"',s.replace('"', '""').replace("\n", "\\n").replace("\r", "\\r"),'"'])


if __name__ == "__main__":
    main()