import os
import sys
import subprocess
import importlib
from pathlib import Path
import csv

run_job = """
#SBATCH --account=YOUR_ACCOUNT
#SBATCH --time=24:00:00
#SBATCH --gres=gpu:1
#SBATCH --mem-per-cpu=64000M

echo "load"
module load python/3.6 cuda cudnn
echo "create env"
virtualenv --no-download  $SLURM_TMPDIR/tensorflow_env
source $SLURM_TMPDIR/tensorflow_env
pip install --no-index --upgrade pip
pip install --no-index tensorflow_gpu numpy pandas matplotlib pyyaml tensorflow_datasets
echo "clone"
git clone . $SLURM_TMPDIR/repo
echo "copy"
cd $SLURM_TMPDIR/repo
echo "run"
pwd
ls
echo "run $(date '+%d/%m/%Y %H:%M:%S')"
$COMMAND
echo "done"
"""

def read_csv(filename, row_index=None):
    with open(filename, "r") as fp:
        data = list(csv.reader(fp))

    if row_index is None:
        return data[1:]
    row = {}
    for i, key in enumerate(data[0]):
        if i == 0 and key == "":
            continue
        row[key] = data[row_index+1][i]
        # parse as int or float
        try:
            row[key] = int(row[key])
        except ValueError:
            try:
                row[key] = float(row[key])
            except ValueError:
                pass
    return row


def submit():
    length = 0
    # if the first argument is a python file or a python function
    if len(sys.argv) >= 2 and (sys.argv[1].endswith(".py") or ".py:" in sys.argv[1]):
        # then the second argument should be a csv file
        try:
            data = read_csv(sys.argv[2])
        except:
            print(f"File {sys.argv[2]} is not a valid csv file.")
            exit()
        length = len(data)
        if length == 0:
            print(f"File {sys.argv[2]} does not contain jobs.")
            exit()
        command = f"pysubmit_start \"{sys.argv[1]}\" \"{sys.argv[2]}\" $SLURM_ARRAY_TASK_ID"
        print(f"Found {length} jobs in {sys.argv[2]}")
    elif len(sys.argv) >= 2:
        # then the second argument should be a file we load with pandas
        if sys.argv[1].endswith(".csv"):
            print("To submit csv files you need to provide a python script file:")
            print("     pysubmit run.py jobs.csv")
            exit()
        try:
            with open(sys.argv[1], "r") as fp:
                length = len(fp.readlines())
        except:
            print(f"File {sys.argv[1]} is not a valid file.")
            exit()
        if length == 0:
            print(f"File {sys.argv[1]} does not contain jobs.")
            exit()
        command = f"pysubmit_start \"{sys.argv[1]}\" $SLURM_ARRAY_TASK_ID"
        print(f"Found {length} jobs in {sys.argv[1]}")
    else:
        print("To submit a list of commands call")
        print("     pysubmit jobs.dat")
        print("To submit a list of python script calls")
        print("     pysubmit run.py jobs.csv")
        print("To submit a list of python function calls")
        print("     pysubmit run.py:main jobs.csv")
        exit()

    try:
        with open("run_job.sh", "r") as fp:
            file_content = fp.read()
    except FileNotFoundError:
        with open("run_job.sh", "w") as fp:
            fp.write(run_job)
        print("ERROR: define job script run_job.sh. I just created a template run_job.sh")
        return

    if "#SBATCH --account=YOUR_ACCOUNT" in file_content:
        print("ERROR: define your own account in run_job.sh")
        return

    file_content = file_content.replace("$COMMAND", "pip install git+https://github.com/rgerum/slurm_job_submitter\n"+command)

    file_content = f"""#!/bin/bash
#SBATCH --array=0-{length}

"""+file_content

    print(file_content)

    with open("job.sh", "w") as fp:
        fp.write(file_content)

    os.system("sbatch job.sh")

def start():
    # if the first argument is a python file or a python function
    if sys.argv[1].endswith(".py") or ".py:" in sys.argv[1]:
        # then the second argument should be a csv file we load with pandas
        data = read_csv(sys.argv[2], int(sys.argv[3]))
        if 0:
            # remove an index column if present
            if "Unnamed: 0" == data.columns[0]:
                data = data.drop(columns=data.columns[0])
            # get the row specified by the third argument
            data = data.iloc[int(sys.argv[3])]
        # is it a python function?
        if ":" in sys.argv[1]:
            # split the command in file and function
            filename, function = sys.argv[1].split(":")
            # import the file
            sys.path.append(str(Path(filename).parent))
            module = importlib.import_module(Path(filename).stem)
            # and call the function
            getattr(module, function)(**data)
        # if it is a python file
        else:
            # assemble the commands to call the file
            commands = ["python", sys.argv[1]]
            for key, value in data.items():
                commands.append(f"--{key}")
                commands.append(f"{value}")
            # and call it
            subprocess.Popen(commands)
    else:
        # if not the file should directly contain commands
        with open(sys.argv[1]) as fp:
            command = fp.readlines()[int(sys.argv[2])]
        print(command)
        os.system(command)
