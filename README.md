# Slurm Job Submitter

The Slurm Job Submitter is a small python package that allows you to easly run python scripts on a SLURM scheduler.

## Installation
You can install it directly from github using pip:

	pip install git+https://github.com/rgerum/slurm_job_submitter

## Usage
First, you need a "run_job.sh" script that defines the SLURM parameters and the commands to run.
You can call 

    pysubmit init

to get an example job script. It needs to contain the variable $COMMAND which
will later be replaced by the actual command to run.

If you use the example job script you need to define which account to use. So change the line
with "#SBATCH --account=YOUR_ACCOUNT". If you try to run a command without a run_job.sh script the default script is
also created.

The slurm job submitter has different modes of operation.

## Submit
### Arbitrary commands
Define a text file with one command in each line, e.g. called "jobs.dat". You can start the 
job submission with

    pysubmit jobs.dat

It will then start each line of the "jobs.dat" file as a command.

See [Example Commands](/examples/commands)

### Python files
Define a csv file defining all the parameter that you want to call, e.g. "jobs.csv", 
with which you want to start the python file, e.g. "file.py". You can start the 
job submission with

    pysubmit file.py jobs.csv

The python file will be called with:

```bash
python file.py --argname1 value1 --argname2 value2 ...
```

See [Example Python Files](/examples/python_files)

### Python functions
Define a csv file defining all the parameter that you want to call, e.g. "jobs.csv", 
with which you want to start the python function, e.g. "main", from a python file, e.g. "file.py". You can start the 
job submission with

    pysubmit file.py:main jobs.csv

The function will be called with the parameters as keyword arguments:

```python
from file import main
main(argname1=value1, argname2=value2, ...)
```

See [Example Python Functions](/examples/python_functions)

## Status
To print the current status of the processes call

    pysubmit status

The status is stored in the file "slurm-list.csv".
## Resubmit
As Slurm Job Submitter stores the status of each job, jobs can also easily be resubmitted.
Just add "resubmit" after the "pysubmit" command and then the python file or data fiale to run.

    pysubmit resubmit run.py jobs.csv

Only the jobs that have not run through completely will (e.g. cancelled, error, or timeout) be restarted. 

## Clear
To clear all slurm job submitter and slurm logs call

    pysubmit clear

adding the "-y" flag removes the prompt.
## Cancel
To cancel all currently running jobs call which where started form this folder

    pysubmit cancel

## Log
Print the log of the Nth job

    pysubmit log N
