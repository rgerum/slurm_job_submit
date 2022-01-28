import csv
from pathlib import Path

def parse_value(value):
    if value == "n/a":
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            pass

def read_csv(file):
    if not Path(file).exists():
        return []
    with open(file, "r") as fp:
        data = list(csv.reader(fp))

    keys = data[0]
    if keys[0] == "":
        keys = keys[1:]
    rows = []
    for d in data[1:]:
        if len(d):
            rows.append({key: value for key, value in zip(keys, d)})
    return rows

def write_csv(file, data):
    keys = []
    for d in data:
        keys.extend([key for key in d.keys() if not key in keys])

    text = ""
    text += ",".join([str(key) for key in list(keys)]) + "\n"
    for d in data:
        text += (",".join([str(d.get(key, "n/a")) if d.get(key, "n/a") is not None else "n/a" for key in keys])+"\n")

    with open(file, "w") as fp:
        fp.write(text)


if __name__ == "__main__":
    write_csv("tmp.csv", [
        dict(a="a"),
        dict(b=10, a=None, c="new"),
    ])
    print(read_csv("tmp.csv"))