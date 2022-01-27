import os
import sys
import subprocess
import importlib
from pathlib import Path
import csv
import argparse
import time
import signal

# default job script that gets created if no job script is present
run_job = """
#SBATCH --account=YOUR_ACCOUNT
#SBATCH --time=24:00:00
#SBATCH --gres=gpu:1
#SBATCH --mem-per-cpu=64000M
#SBATCH --signal=B:TERM@00:05

# store start time
start=`date +%s.%N`

# load modules
echo "load"
module load python/3.6 cuda cudnn

# create and activate virtual environment
echo "create env"
virtualenv --no-download  $SLURM_TMPDIR/tensorflow_env
source $SLURM_TMPDIR/tensorflow_env/bin/activate

# install python packages
pip install --no-index --upgrade pip
pip install --no-index tensorflow_gpu numpy pandas matplotlib pyyaml tensorflow_datasets

# store end time of preparation
end_prep=`date +%s.%N`
runtime_prep=$( echo "$end_prep - $start" | bc -l )
echo "Preparation finished. Execution took $runtime_prep seconds. $(date '+%d/%m/%Y %H:%M:%S')"

# running main command
$COMMAND

# store end time and print finished message
end=`date +%s.%N`
runtime=$( echo "$end - $start" | bc -l )
echo "Done. Execution took $runtime seconds."
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
    if len(sys.argv) >= 1 and sys.argv[1] == "init":
        with open("run_job.sh", "w") as fp:
            fp.write(run_job)
        print("File run_job.sh created.")
        exit()
    if len(sys.argv) >= 1 and sys.argv[1] == "status":
        os.system("cat slurm-list.csv")
        exit()
    if len(sys.argv) >= 1 and sys.argv[1] == "start":
        sys.argv.pop(1)
        start()
        exit()
    # if the first argument is a python file or a python function
    elif len(sys.argv) >= 2 and (sys.argv[1].endswith(".py") or ".py:" in sys.argv[1]):
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
        command = f"pysubmit_start \"{sys.argv[1]}\" \"{sys.argv[2]}\" $SLURM_ARRAY_TASK_ID --slurm_id $SLURM_ARRAY_JOB_ID"
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
        command = f"pysubmit_start \"{sys.argv[1]}\" $SLURM_ARRAY_TASK_ID --slurm_id $SLURM_ARRAY_JOB_ID"
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

    submit = subprocess.check_output(["sbatch", "job.sh"])

    try:
        batch_id = int(submit.split()[-1])
        print("batch_id", batch_id)
        for i in range(length):
            set_job_status(batch_id, i, [start_time, time.time(), -1, "submitted"])
    except ValueError as err:
        raise err


def set_job_status(slurm_id, index, status):
    id = f"{slurm_id}_{index}"
    while True:
        try:
            with open(f"slurm-lock", "w") as fp0:
                if Path("slurm-list.csv").exists():
                    with open(f"slurm-list.csv", "r") as fp:
                        data = list(csv.reader(fp))
                else:
                    data = []

                if len(data) == 0:
                    data.append(["id", "start_time", "end_time", "status", "status_text"])

                index = 0
                for index in range(len(data)):
                    if len(data[index]) and data[index][0] == id:
                        break
                else:
                    data.append([])
                    index = len(data)-1
                data[index] = [id]+status
                with open(f"slurm-list.csv", "w") as fp:
                    for row in data:
                        fp.write(",".join([str(r) for r in row])+"\n")
            break
        except IOError as err:
            print(err, "waiting for slurm-lock")
            time.sleep(0.001)

def start():
    parser = argparse.ArgumentParser(description='Call a python script of function.')
    if sys.argv[1].endswith(".py") or ".py:" in sys.argv[1]:
        parser.add_argument('script', type=str, help='the script to call')
    parser.add_argument('datafile', type=str, help='the csv file from which to take the data')
    parser.add_argument('index', type=int, help='the csv file from which to take the data')
    parser.add_argument('--slurm_id', type=str, default=None, help='the id of the slurm process')

    args = parser.parse_args()
    print(args)

    # Definition of the signal handler. All it does is flip the 'interrupted' variable
    def signal_handler(signum, frame):
        if args.slurm_id is not None:
            set_job_status(args.slurm_id, args.index, [start_time, time.time(), -1, "cancel"])

    # Register the signal handler
    signal.signal(signal.SIGTERM, signal_handler)

    if args.slurm_id is not None:
        start_time = time.time()
        set_job_status(args.slurm_id, args.index, [start_time, -1, -1, "running"])

    try:
        # if the first argument is a python file or a python function
        if getattr(args, "script", None) is not None:

            # then the second argument should be a csv file we load with pandas
            data = read_csv(args.datafile, args.index)

            # is it a python function?
            if ":" in args.script:
                # split the command in file and function
                filename, function = args.script.split(":")
                # import the file
                sys.path.append(str(Path(filename).parent))
                module = importlib.import_module(Path(filename).stem)
                # and call the function
                getattr(module, function)(**data)
            # if it is a python file
            else:
                # assemble the commands to call the file
                commands = ["python", args.script]
                for key, value in data.items():
                    commands.append(f"--{key}")
                    commands.append(f"{value}")
                # and call it
                subprocess.check_call(commands, shell=False)
        else:
            # if not the file should directly contain commands
            with open(args.datafile) as fp:
                command = fp.readlines()[args.index]
            print(command)
            subprocess.check_call(command, shell=True)
            #os.system(command)
    except subprocess.CalledProcessError as err:
        if args.slurm_id is not None:
            set_job_status(args.slurm_id, args.index, [start_time, time.time(), 0, "error"])
        raise

    if args.slurm_id is not None:
        set_job_status(args.slurm_id, args.index, [start_time, time.time(), 0, "done"])