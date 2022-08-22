from json import loads
from csv import writer

# ASCII ID for less memory, from 32-126, skipping 44 (comma)
# uses much less memory
# almost no added time
def increment(id):
    for i in range(len(id) - 1, -1, -1):
        x = id[i]
        if x != "~":
            return "".join([id[:i], ("-" if x == "+" else chr(ord(x) + 1)), id[i+1:]])
        id = "".join([id[:i], (" "), id[i+1:]])
    # new character
    return "".join([" ", id])

sites = writer(open("sites.csv", "w"))
links = writer(open("links.csv", "w"))

# headers

sites.writerow(["id", "url", "title"]) 
# if a title is not known, it will be an empty string
# in the search engine, the url can be used, but doing that here takes much more space

links.writerow(["start", "end"])
# written in IDs

done, id = 0, " "

with open("TestData.wat", encoding="utf-8") as f:
    # set buffer, skip header
    for i in range(44):
        f.readline()

    try:
        while True:
            data = loads(f.readline())['Envelope']
            url = data['WARC-Header-Metadata']['WARC-Target-URI']
            curid = id
            id = increment(id)
            try:
                title = data['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Title']
                linkbook = data['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
                sites.writerow([id, url, title])
                id = increment(id)
                for link in linkbook:
                    l = "" # the link
                    if "url" in link:
                        l = link["url"]
                    elif link:
                        # links always include "href" or "url" unless they are empty
                        l = link["href"]

                    if "http" in l:
                        # this is a link to a site or image
                        # we need to make sure it gets included
                        # if it is a duplicate, that's ok, it'll get filtered
                        sites.writerow([id, l, ""])
                        links.writerow([curid, id])
                        id = increment(id)
                    elif l[0] == "/":
                        # a directory (ie /images)
                        sites.writerow([id, "".join([url if url[-1] != "/" else url[:-1], l]) , ""]) # also must be included, just in case (think stack overflow)
                        links.writerow([curid, id])
                        id = increment(id)
                    # Anything else is somehting like javascript or php,
                    # which is not accessed by a search engine

            except:
                # site does not have HTML Metadata (no title, no links)
                sites.writerow([url, ""])
            if done % 1000 == 0:
                print(str(done) + " sites loaded")
            for i in range(32):
                f.readline()
    except:
        #file ended
        pass