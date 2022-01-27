import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--repetition', type=int)
parser.add_argument('--strength', type=float)

args = parser.parse_args()
print("I am called with", args)
