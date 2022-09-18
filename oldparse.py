from orjson import loads # 6x faster than built-in
from time import time

STARTING_ROW, JUMP = 44, 32

def main():
    sites = open("sites.csv", "w")
    links = open("links.csv", "w")

    # headers

    sites.write(":ID,url,title\n")
    # if a title is not known, it will be an empty string
    # in the search engine, the url can be used, but doing that here takes much more space

    links.write(":START_ID,:END_ID\n")
    # written in IDs
    
    with open("TestData.wat", encoding="utf-8") as f:
        # set buffer, skip header
        for i in range(STARTING_ROW):
            next(f)
        try:
            id = "!"
            while True:
                data = loads(f.readline())['Envelope']
                url = q(data['WARC-Header-Metadata']['WARC-Target-URI'])
                try:
                    title = q(data['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Title'])
                    sites.write(",".join([id, url, title]) + "\n")
                    curid = id
                    id = increment(id)

                    linkbook = data['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
                    swrite = []
                    lwrite = []
                    for link in linkbook:
                        l = "" # the link
                        if "url" in link:
                            l = link["url"]
                        elif link:
                            # links always include "href" or "url" unless they are empty
                            l = link["href"]

                        if len(l) >= 2 and l[0] == "/" and l[1] != "/":
                            # two slashes seems to be an API connection
                            # but one slash is a directpry
                            swrite.append(",".join([id, "".join([url[:-1] if url[-2] != "/" else url[:-2], q(l)[1:]])]) + ",\n")
                            # also must be included, just in case (think stack overflow)
                            lwrite.append(",".join([curid, id]) + "\n")
                        elif len(l) >= 8 and (l[:7] == "http://" or l[:8] == "https://"):
                            # this is a link to a site or image
                            # we need to make sure it gets included
                            # if it is a duplicate, that's ok, it'll get filtered
                            swrite.append(",".join([id, q(l)]) + ",\n")
                            lwrite.append(",".join([curid, id]) + "\n")
                        # Anything else is somehting like javascript or php,
                        # which is not accessed by a search engine
                    sites.writelines(swrite)
                    links.writelines(lwrite)

                except:
                    # site does not have HTML Metadata (no title, no links)
                    sites.write(",".join([id, url]) + ",\n")
                    id = increment(id)
                for i in range(JUMP):
                    next(f)
        except StopIteration:
            # file ended
            pass

    sites.close()
    links.close()

# ASCII ID for less memory, from 33-126 (because of whitespace trimming), 
# skipping 22, 34, and 92, comma, quote, and backslash (ex comma, between '-' and '+' on ASCII chart)
# uses much less memory with almost no added time
def increment(id):
    for i in range(len(id) - 1, -1, -1):
        x = id[i]
        if x != "~":
            return "".join([id[:i], ("-" if x == "+" else ("#" if x == "!" else ("]" if x == "[" else chr(ord(x) + 1)))), id[i+1:]])
        id = "".join([id[:i], "!", id[i+1:]])
    # new character
    return "".join(["!", id])

# wrap in quotes and protect quotes and newlines, shorthand
def q(s):
    return "".join(['"',s.replace('"', '""').replace("\n", "\\n").replace("\r", "\\r"),'"'])


if __name__ == "__main__":
    start = time()
    main()
    print(time() - start)