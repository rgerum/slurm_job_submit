from setuptools import setup

setup(name='slurm_job_submit',
      version="0.1",
      packages=['slurm_job_submit'],
      description='Submit SLURM jobs',
      author='Richard Gerum',
      author_email='richard.gerum@fau.de',
      license='MIT',
      entry_points={
          'console_scripts': ['pysubmit=slurm_job_submit.submit:submit', 'pysubmit_start=slurm_job_submit.submit:start'],
      },
      install_requires=[],
)
