# default job script that gets created if no job script is present
run_job = """
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