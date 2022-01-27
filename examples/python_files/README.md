# Example with python script

Schedule SLURM execution with
    
    pysubmit run.py jobs.csv

Or for testing call, e.g. the third, command directly with

    pysubmit_start run.py jobs.csv 3

## Files
Here the jobs.csv file contains a table with all the parameter combinations
for which to run the "run.py" script.

```
,repetition,strength
0,0,0.0
1,0,0.001
2,0,0.01
3,0,0.1
4,0,1.0
5,1,0.0
6,1,0.001
7,1,0.01
8,1,0.1
9,1,1.0
10,2,0.0
11,2,0.001
12,2,0.01
13,2,0.1
14,2,1.0
```

So e.g. the third command gets executed as:

    python run.py --repetition 3 --strength 0.1

The run.py file can parse the arguments with argparse. 

```python
import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--repetition', type=int)
parser.add_argument('--strength', type=float)

args = parser.parse_args()
print("I am called with", args)
```