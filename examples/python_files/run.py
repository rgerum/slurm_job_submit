import argparse
from slurm_job_submitter import set_job_status

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--repetition', type=int)
parser.add_argument('--strength', type=float)

args = parser.parse_args()
print("I am called with", args)
set_job_status(dict(process="bla"))
