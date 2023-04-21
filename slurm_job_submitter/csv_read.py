import csv
from pathlib import Path
import os
import fcntl

SLURM_LIST = "slurm-list.csv"
SLURM_LOCK = "slurm-lock"


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


def read_csv(filename):
    if isinstance(filename, (Path, str)):
        if not Path(filename).exists():
            return []
        with open(filename, "r") as fp:
            data = list(csv.reader(fp))
    else:
        data = list(csv.reader(filename))

    if len(data) <= 1:
        return []

    keys = data[0]
    rows = []
    for d in data[1:]:
        if len(d):
            rows.append({key: value for key, value in zip(keys, d) if key != ""})
    return rows


def write_csv(file, data):
    keys = []
    for d in data:
        keys.extend([key for key in d.keys() if key not in keys])

    text = ""
    text += ",".join([str(key) for key in list(keys)]) + "\n"
    for d in data:
        if d["id"] is not None:
            text += (",".join([str(d[key]) if d.get(key, None) is not None else "n/a" for key in keys])+"\n")

    with open(file, "w") as fp:
        fp.write(text)


class Lock:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.fp = open(SLURM_LOCK, "w")
        fcntl.lockf(self.fp, fcntl.LOCK_EX)
        return self.fp

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fp.close()


def set_job_status(status, index=None):
    if index is None:
        id = os.environ["SJS_SLURM_JOB_ID"]
    else:
        id = str(index)
    # convert the id to an integer
    id = int(id)

    with Lock(SLURM_LOCK):
        data = read_csv(SLURM_LIST)

        for index in range(len(data)):
            if int(data[index].get("id", None)) == int(id):
                data[index].update(status)
                break
        else:
            data.append(status)

        write_csv(SLURM_LIST, data)
