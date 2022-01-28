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


def set_job_status(status, slurm_id=None, index=None):
    if index is None:
        id = os.environ["SJS_SLURM_JOB_ID"]
    else:
        id = str(index)

    while True:
        try:
            with open(f"slurm-lock", "w") as fp0:
                data = read_csv(SLURM_LIST)

                for index in range(len(data)):
                    if int(data[index].get("id", None)) == int(id):
                        data[index].update(status)
                        break
                else:
                    data.append(status)

                write_csv(SLURM_LIST, data)
            break
        except IOError as err:
            print(err, "waiting for slurm-lock")
            time.sleep(0.001)
