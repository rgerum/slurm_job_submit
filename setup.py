from setuptools import setup

setup(name='slurm_job_submitter',
      version="0.1",
      packages=['slurm_job_submitter'],
      description='Submit SLURM jobs',
      author='Richard Gerum',
      author_email='richard.gerum@fau.de',
      license='MIT',
      entry_points={
          'console_scripts': ['pysubmit=slurm_job_submitter.submit:main', 'pysubmit_start=slurm_job_submitter.submit:start'],
      },
      install_requires=[],
)
