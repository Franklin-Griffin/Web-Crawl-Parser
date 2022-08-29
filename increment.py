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