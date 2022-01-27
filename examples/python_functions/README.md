# Example with python function

Schedule SLURM execution with
    
    pysubmit run.py:main jobs.csv

Or for testing call, e.g. the third, command directly with

    pysubmit_start run.py:main jobs.csv 3

## Files
Here the jobs.csv file contains a table with all the parameter combinations
for which to run the function "main" in the "run.py" script.

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

    main(repetition=3, strength=0.1)

The python file "run.py" should then define the specified function "main".
If you want to also be able to execute the file directly you can add a `if __name__ == "__main__":` part. 
```python
def main(repetition, strength):
    import sys
    print(f"I am called with repetition={repetition}, strength={strength}")

# hide executions that you only want to execute when the script is run directly behind if __name__ == "__main__"
if __name__ == "__main__":
    # execute it if you call the file directly
    main(1, 0.3)
```