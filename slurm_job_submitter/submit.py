import os
import sys
import subprocess
import importlib
from pathlib import Path
import csv
import argparse
import time
import signal
import datetime

from .default_jobscript import run_job
from .csv_read import read_csv, write_csv, set_job_status, SLURM_LIST


def init():
    """
    pysubmit init
    This command creates a run_job file
    """
    keys = {"time": "24:00:00",
            "gres": "gpu:1",
            "mem-per-cpu": "64000M",
            }
    index = 1
    shortcuts = {"A": "account", "t": "time", "a": "array"}
    while index < len(sys.argv):
        arg = sys.argv[index]
        if arg.startswith("--"):
            key, value = arg[2:].split("=", 1)
            keys[key] = value
        elif arg.startswith("-"):
            key = arg[1:]
            value = sys.argv[index+1]
            if key in shortcuts:
                key = key[shortcuts]
            keys[key] = value
            index += 1
        index += 1
    if "account" not in keys:
        print("Add command line arguments for the SBATCH arguments.")
        print("For example:")
        print("pysubmit init --account=YOUR_ACCOUNT --time=24:00:00 --gres=gpu:1 --mem-per-cpu=64000M")
        print("The account argument is required, the other ones are optional.")
        exit()
    if "array" in keys:
        print("Argument ARRAY is not allowed, it will be filled automatically.")
        exit()
    with open("run_job.sh", "w") as fp:
        for key, value in keys.items():
            fp.write(f"#SBATCH --{key}={value}\n")
        fp.write("\n")
        fp.write(run_job.strip()+"\n")
    print("File run_job.sh created.")
    exit()


def log():
    """
    pysubmit log N
    This command prints the log from job number N
    """
    if len(sys.argv) < 3:
        print("Provide a job number")
        exit()
    try:
        # read the job status list
        data = read_csv(SLURM_LIST)
        d = data[int(sys.argv[2])]
    except ValueError:
        print(f"{sys.argv[2]} is not a valid number")
        exit()
    except IndexError:
        print(f"No job with id {sys.argv[2]}")
        exit()
    os.system(f"cat slurm-{d['job_id']}.out")
    exit()


def cancel():
    """
    pysubmit cancel
    This command cancels all the current jobs.
    """
    # read the job status list
    data = read_csv(SLURM_LIST)
    # get the job ids
    job_ids = {d["job_id"].split("_")[0] for d in data if d["status_text"] in ["pending", "running"]}
    # cancel them
    try:
        subprocess.check_output(["scancel", *job_ids])  # "--signal=TERM"
    except subprocess.CalledProcessError:
        # omit the python error here as sbatch already should have printed an error message
        pass
    exit()


def clear():
    """
    pysubmit clear
    This command clears all the output from slurm job submitter and slurm
    """
    files = [r for r in Path(".").glob("slurm*")]
    if len(files) == 0:
        print("No files to clear")
        exit()
    print("Remove the files")
    for file in files:
        print("  ", file)
    print(f"Do you want to delete all the listed files? (y/n)")
    if "-y" in sys.argv or input() == "y":
        os.system("rm slurm*")
        print(f"Removed {len(files)} files")
    exit()


def status():
    """
    pysubmit status
    This command prints the current status of all jobs.
    """
    if len(sys.argv) >= 3 and sys.argv[2] == "update":
        import io
        # read the job status list
        data = read_csv(SLURM_LIST)

        # get the job ids
        job_ids = list({d["job_id"].split("_")[0] for d in data})

        output = subprocess.check_output(['squeue', '-o', '"%i, %t, %T"', '-j', ",".join(job_ids)])
        output = read_csv(io.StringIO(output.decode().replace('"', '')))

        slurm_states = {}
        for d in output:
            array_id, id = d["JOBID"].split("_", 1)
            if id.startswith("["):
                start, end = id[1:-1].split("-")
                id = range(int(start), int(end))
            else:
                id = [id]
            for i in id:
                slurm_states[str(i)] = d["STATE"]
        for d in data:
            state = slurm_states.get(str(d["id"]), "CANCELLED")
            if d["status_text"] == "running" and state != "RUNNING":
                d["status_text"] = state.lower()
            elif d["status_text"] == "pending" and state != "PENDING":
                d["status_text"] = state.lower()
        write_csv(SLURM_LIST, data)
    if Path(SLURM_LIST).exists():
        # print the table
        os.system("cat slurm-list.csv | column -t -s,")

        # read the job status list
        data = read_csv(SLURM_LIST)
        values_counts = {}
        total_count = 0
        for d in data:
            if d["status_text"] not in values_counts:
                values_counts[d["status_text"]] = 0
            values_counts[d["status_text"]] += 1
            total_count += 1
        # print the summary
        print(f"Summary: {total_count} jobs", *[f"{value} {key}" for key, value in values_counts.items()])
    else:
        print("No jobs submitted yet.")
    exit()

def resubmit():
    """
    pysubmit resubmit SCRIPT DATAFILE
    pysubmit resubmit DATAFILE
    Resubmit only jobs that have not been finished successfully yet.
    """
    # read the job status list
    data = read_csv(SLURM_LIST)

    # filter only the unfinished jobs
    array_list = [int(d["id"]) for d in data if d["status"] == "-1"]

    if len(array_list) == 0:
        print("No jobs to resubmit")
        exit()

    # print the list
    array_command = ",".join([str(s) for s in array_list])
    print(f"resubmitting {len(array_list)} jobs:", array_command)
    sys.argv.pop(1)

    submit(array_list, array_command)


def submit(array_list=None, array_command=None):
    """
    pysubmit submit SCRIPT DATAFILE
    pysubmit submit DATAFILE
    Submit all jobs found in DATAFILE.
    """
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

        commands = []
        for data in read_csv(sys.argv[2]):
            if ":" in sys.argv[1]:
                commands.append(sys.argv[1] + "(" + " ".join([f"{key}={value}" for key, value in data.items()]) + ")")
            else:
                # assemble the commands to call the file
                cmd = [sys.argv[1]]
                for key, value in data.items():
                    cmd.append(f"--{key}")
                    cmd.append(f"{value}")
                commands.append(" ".join(cmd))
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
        with open(sys.argv[1]) as fp:
            commands = fp.readlines()
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
            file_content = fp.read().strip()
    except FileNotFoundError:
        print("ERROR: You need to define job script run_job.sh.\nCall \npysubmit init\n to create one.")
        return

    if "#SBATCH --account=YOUR_ACCOUNT" in file_content:
        print("ERROR: define your own account in run_job.sh")
        return

    # remove shebang if present
    file_content.replace("#!/bin/bash\n", "")

    if array_list is None:
        array_command = f"0-{length}"
        array_list = range(length)

    file_content = file_content.replace("$COMMAND",
                                        "pip install git+https://github.com/rgerum/slurm_job_submitter\n" + command)

    file_content = f"""#!/bin/bash
#SBATCH --array={array_command}
""" + file_content + "\n"

    # print(file_content)

    with open("job.sh", "w") as fp:
        fp.write(file_content)

    try:
        submit = subprocess.check_output(["sbatch", "job.sh"])
        print(submit)
    except subprocess.CalledProcessError:
        # omit the python error here as sbatch already should have printed an error message
        return

    batch_id = int(submit.split()[-1])

    for i in range(length):
        if i in array_list:
            set_job_status(
                dict(id=i, job_id=f"{batch_id}_{i}", start_time=None, end_time=None, duration=None, status=-1,
                     status_text="pending", command=commands[i]), batch_id, i)


def start():
    """
    pysubmit start SCRIPT DATAFILE JOB_NUMBER
    pysubmit start DATAFILE JOB_NUMBER
    Start the job JOB_NUMBER from DATAFILE.
    """
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
            set_job_status(dict(end_time=datetime.datetime.now(), duration=datetime.datetime.now() - start_time,
                                status_text="cancel"), args.slurm_id, args.index)

    # Register the signal handler
    signal.signal(signal.SIGTERM, signal_handler)

    if args.slurm_id is not None:
        os.environ["SJS_SLURM_ID"] = str(args.slurm_id)
    os.environ["SJS_SLURM_JOB_ID"] = str(args.index)
    start_time = datetime.datetime.now()
    set_job_status(dict(start_time=start_time, status_text="running"), args.slurm_id, args.index)

    try:
        # if the first argument is a python file or a python function
        if getattr(args, "script", None) is not None:

            # then the second argument should be a csv file we load with pandas
            data = read_csv(args.datafile)[args.index]

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
            set_job_status(dict(end_time=datetime.datetime.now(), duration=datetime.datetime.now() - start_time,
                                status_text="error"), args.slurm_id, args.index)
        raise

    set_job_status(dict(end_time=datetime.datetime.now(), duration=datetime.datetime.now() - start_time, status=0,
                        status_text="done"), args.slurm_id, args.index)


def main():
    if len(sys.argv) >= 1:
        if sys.argv[1] == "init":
            return init()
        elif sys.argv[1] == "log":
            return log()
        elif sys.argv[1] == "cancel":
            return cancel()
        elif sys.argv[1] == "clear":
            return clear()
        elif sys.argv[1] == "status":
            return status()
        elif sys.argv[1] == "submit":
            sys.argv.pop(1)
            return submit()
        elif sys.argv[1] == "resubmit":
            return resubmit()
        elif sys.argv[1] == "start":
            sys.argv.pop(1)
            return start()
    return submit()
