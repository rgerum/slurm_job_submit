import os
import sys
import io
import subprocess
import importlib
from pathlib import Path
import signal
import datetime

import fire

from .default_jobscript import run_job
from .csv_read import read_csv, write_csv, set_job_status, SLURM_LIST


def repo_path():
    return Path(__file__).parent


class Submitter:
    @staticmethod
    def init(**kwargs):
        """
        This command creates a run_job file
        """
        keys = {"time": "24:00:00",
                "gres": "gpu:1",
                "mem-per-cpu": "64000M",
                }
        # convert shortcuts
        shortcuts = {"A": "account", "t": "time", "a": "array"}
        for key, value in kwargs.items():
            if key in shortcuts:
                key = shortcuts[key]
            keys[key] = value

        # error when account is not provided
        if "account" not in keys:
            print("Add command line arguments for the SBATCH arguments.")
            print("For example:")
            print("pysubmit init --account=YOUR_ACCOUNT --time=24:00:00 --gres=gpu:1 --mem-per-cpu=64000M")
            print("The account argument is required, the other ones are optional.")
            exit()
        # error if array is preset
        if "array" in keys:
            print("Argument ARRAY is not allowed, it will be filled automatically.")
            exit()
        # add arguments to run_job.sh and write it
        with open("run_job.sh", "w") as fp:
            for key, value in keys.items():
                fp.write(f"#SBATCH --{key}={value}\n")
            fp.write("\n")
            fp.write(run_job.strip()+"\n")
        print("File run_job.sh created.")

    @staticmethod
    def log(index: int):
        """
        This command prints the log from job number N
        """
        try:
            # read the job status list
            data = read_csv(SLURM_LIST)
            d = data[int(index)]
        except ValueError:
            print(f"Index needs to be an integer number. {index} is not a valid number")
            exit()
        except IndexError:
            print(f"No job with id {index}")
            exit()
        os.system(f"cat slurm-{d['job_id']}.out")

    @staticmethod
    def cancel():
        """
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

    @staticmethod
    def clear(y: bool = None):
        """
        This command clears all the output from slurm job submitter and slurm
        """
        # find all the slurm output files
        files = [r for r in Path(".").glob("slurm*")]

        # if no files are present
        if len(files) == 0:
            print("No files to clear")
            exit()

        # list all the files
        print("Remove the files")
        for file in files:
            print("  ", file)

        # if y is not provided ask to delete
        if y is None:
            print(f"Do you want to delete all the listed files? (y/n)")
            y = input()

        # if yes remove them
        if y:
            os.system("rm slurm*")
            print(f"Removed {len(files)} files")

    @staticmethod
    def status(update=False):
        """
        This command prints the current status of all jobs.
        """
        if update:
            # read the job status list
            data = read_csv(SLURM_LIST)

            # get the job ids
            job_ids = list({d["job_id"].split("_")[0] for d in data})

            output = subprocess.run(['squeue', '-o', '"%i,%t,%T"', '-j', ",".join(job_ids)], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            if output.stderr.decode():
                # if the job already ran completely the call might crash
                if "Invalid job id specified" in output.stderr.decode():
                    output = []
                else:
                    # subprocess.CalledProcessError
                    raise ValueError(output.stderr.decode())
            else:
                output = output.stdout.decode()
                output = read_csv(io.StringIO(output.replace('"', '')))

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

    @staticmethod
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

        Submitter.submit(array_list=array_list, array_command=array_command)

    @staticmethod
    def update():
        if (repo_path() / "slurm_job_submitter").exists():
            os.system("cd slurm_job_submitter")
            os.system("git pull")
            os.system("cd ..")
        else:
            os.system("git clone https://github.com/rgerum/slurm_job_submitter")

    @staticmethod
    def submit(script_file=None, csv_file=None, array_list=None, array_command=None):
        """
        pysubmit submit SCRIPT DATAFILE
        pysubmit submit DATAFILE
        Submit all jobs found in DATAFILE.
        """
        length = 0

        # if the first argument is a python file or a python function
        if csv_file is not None and (script_file.endswith(".py") or ".py:" in script_file):
            # then the second argument should be a csv file
            try:
                data = read_csv(csv_file)
            except:
                print(f"File {csv_file} is not a valid csv file.")
                exit()
            length = len(data)
            if length == 0:
                print(f"File {csv_file} does not contain jobs.")
                exit()

            commands = []
            for data in read_csv(csv_file):
                if ":" in script_file:
                    commands.append(script_file + "(" + " ".join([f"{key}={value}" for key, value in data.items()]) + ")")
                else:
                    # assemble the commands to call the file
                    cmd = [script_file]
                    for key, value in data.items():
                        cmd.append(f"--{key}")
                        cmd.append(f"{value}")
                    commands.append(" ".join(cmd))
            command = f"pysubmit start \"{script_file}\" \"{csv_file}\" $SLURM_ARRAY_TASK_ID --slurm_id $SLURM_ARRAY_JOB_ID"
            print(f"Found {length} jobs in {csv_file}")
        elif script_file is not None:
            data_file = script_file
            # then the second argument should be a file we load with pandas
            if data_file.endswith(".csv"):
                print("To submit csv files you need to provide a python script file:")
                print("     pysubmit run.py jobs.csv")
                exit()
            try:
                with open(data_file, "r") as fp:
                    length = len(fp.readlines())
            except:
                print(f"File {data_file} is not a valid file.")
                exit()
            if length == 0:
                print(f"File {data_file} does not contain jobs.")
                exit()
            with open(data_file) as fp:
                commands = fp.readlines()
            command = f"pysubmit start \"{data_file}\" $SLURM_ARRAY_TASK_ID --slurm_id $SLURM_ARRAY_JOB_ID"
            print(f"Found {length} jobs in {data_file}")
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

        #if not (repo_path() / "slurm_job_submitter").exists():
        #    Submitter.update()

        if "virtualenv" in file_content:
            file_content = file_content.replace("$COMMAND",
                                                f"pip install {repo_path()}/slurm_job_submitter\n" + command)
        else:
            file_content = file_content.replace("$COMMAND", command)

        # add the array command to the file content
        file_content = f"#!/bin/bash\n#SBATCH --array={array_command}\n" + file_content + "\n"

        with open(".tmp_job.sh", "w") as fp:
            fp.write(file_content)

        try:
            submit = subprocess.check_output(["sbatch", ".tmp_job.sh"])
            print(submit.decode())
        except subprocess.CalledProcessError:
            # omit the python error here as sbatch already should have printed an error message
            return

        batch_id = int(submit.split()[-1])

        for i in range(length):
            if i in array_list:
                set_job_status(
                    dict(id=i, job_id=f"{batch_id}_{i}", start_time=None, end_time=None, duration=None, status=-1,
                         status_text="pending", command=commands[i]), i)

    @staticmethod
    def start(script=None, datafile=None, index=None, slurm_id=None):
        """
        pysubmit start SCRIPT DATAFILE JOB_NUMBER
        pysubmit start DATAFILE JOB_NUMBER
        Start the job JOB_NUMBER from DATAFILE.
        """

        # set the environment variables so that the script could react on this
        os.environ["SJS_SLURM_JOB_ID"] = str(index)
        if slurm_id is not None:
            os.environ["SJS_SLURM_ID"] = str(slurm_id)

        # Definition of the signal handler. All it does is flip the 'interrupted' variable
        def signal_handler(signum, frame):
            if slurm_id is not None:
                set_job_status(dict(end_time=datetime.datetime.now(), duration=datetime.datetime.now() - start_time,
                                    status_text="cancel"), index)

        # Register the signal handler
        signal.signal(signal.SIGTERM, signal_handler)

        start_time = datetime.datetime.now()
        set_job_status(dict(start_time=start_time, status_text="running"), index)

        try:
            # if the first argument is a python file or a python function
            if script is not None:

                # then the second argument should be a csv file we load with pandas
                data = read_csv(datafile)[index]

                # is it a python function?
                if ":" in script:
                    # split the command in file and function
                    filename, function = script.split(":")
                    # import the file
                    sys.path.append(str(Path(filename).parent))
                    module = importlib.import_module(Path(filename).stem)
                    # and call the function
                    getattr(module, function)(**data)
                # if it is a python file
                else:
                    # assemble the commands to call the file
                    commands = ["python", script]
                    for key, value in data.items():
                        commands.append(f"--{key}")
                        commands.append(f"{value}")
                    # and call it
                    subprocess.check_call(commands, shell=False)
            else:
                # if not the file should directly contain commands
                with open(datafile) as fp:
                    command = fp.readlines()[index]
                print(command)
                subprocess.check_call(command, shell=True)
                #os.system(command)
        except subprocess.CalledProcessError as err:
            if slurm_id is not None:
                set_job_status(dict(end_time=datetime.datetime.now(), duration=datetime.datetime.now() - start_time,
                                    status_text="error"), index)
            raise

        set_job_status(dict(end_time=datetime.datetime.now(), duration=datetime.datetime.now() - start_time, status=0,
                            status_text="done"), index)


def main():
    fire.Fire(Submitter)
