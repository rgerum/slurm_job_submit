# Slurm Job Submit

The Slurm Job Submitter is a small python package that allows you to easly run python scripts on a SLURM scheduler.

## Installation
You can install it directly from github using pip:

	pip install git+https://github.com/rgerum/slurm_job_submitter

## Usage
The slurm job submitter has different modes of operation.

### Arbitrary commands
Define a text file with one command in each line, e.g. called "jobs.dat". You can start the 
job submission with

    pysubmit jobs.dat

It will then start each line of the "jobs.dat" file as a command.

### Python files
Define a csv file defining all the parameter that you want to call, e.g. "jobs.csv", 
with which you want to start the python file, e.g. "file.py". You can start the 
job submission with

    pysubmit file.py jobs.csv

The python file will be called with:

    python file.py --argname1 value1 --argname2 value2 ...

### Python functions
Define a csv file defining all the parameter that you want to call, e.g. "jobs.csv", 
with which you want to start the python function, e.g. "main", from a python file, e.g. "file.py". You can start the 
job submission with

    pysubmit file.py:main jobs.csv

The function will be called with the parameters as keyword arguments:

    main(argname1=value1, argname2=value2, ...)

