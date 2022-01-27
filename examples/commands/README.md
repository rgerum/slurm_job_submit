# Example with list of commands

Schedule SLURM execution with
    
    pysubmit jobs.dat

Or for testing call, e.g. the third, command directly with

    pysubmit_start jobs.dat 3

## Files
Here the jobs.dat file contains arbitrary commands. It can also be used to call
non python programs.

    python run.py --repetition 0 --strength 0
    python run.py --repetition 0 --strength 0.001
    python run.py --repetition 0 --strength 0.01
    python run.py --repetition 0 --strength 0.1
    python run.py --repetition 0 --strength 1
    python run.py --repetition 1 --strength 0
    python run.py --repetition 1 --strength 0.001
    python run.py --repetition 1 --strength 0.01
    python run.py --repetition 1 --strength 0.1
    python run.py --repetition 1 --strength 1
    python run.py --repetition 2 --strength 0
    python run.py --repetition 2 --strength 0.001
    python run.py --repetition 2 --strength 0.01
    python run.py --repetition 2 --strength 0.1
    python run.py --repetition 2 --strength 1

