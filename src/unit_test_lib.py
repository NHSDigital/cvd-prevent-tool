# Databricks notebook source
## unit_test_lib

## Overview
# Notebook for functions for performing unit tests in the `run_unit_tests` notebook

# COMMAND ----------

import datetime
import time

from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from typing import List

# COMMAND ----------

def run_unit_tests(test_notebook_list, parent_path: str = 'tests/unit_tests'):
  '''run_unit_tests
  Function to handle the concurrent running of unit tests and capture of (A) Errors (B) Notebooks
  that have failed. Times execution of unit tests.

  Args:
    test_notebook_list (List[str]): List of unit test notebooks (in parent_path) to run.
    parent_path (str, optional): The folder containing the unit test notebooks. Defaults to tests/unit_tests.

  '''
  ## COUNTERS
  run_stage = 0
  run_pass = 0
  run_fail = 0
  notebooks_fail = []
  ## ITERATE
  print('-' * 80)
  print(f'Concurrent unit test run starting at {datetime.datetime.now()}')
  print('-' * 80)
  start_time = time.time()
  for notebook in test_notebook_list:
    run_stage += 1
    ind_start_time = time.time()
    try:
      dbutils.notebook.run('../'+parent_path+'/'+notebook,0)
      # PASS
      run_pass += 1
      ind_end_time = time.time()
      print(f'> ({run_stage}/{len(test_notebook_list)}) [PASSED] {notebook} ({round((ind_end_time - ind_start_time),2)}s)')
    except:
      # FAIL
      run_fail += 1
      notebooks_fail.append(notebook)
      ind_end_time = time.time()
      print(f'> ({run_stage}/{len(test_notebook_list)}) [FAILED] {notebook} ({round((ind_end_time - ind_start_time),2)}s)')
  # Update console
  end_time = time.time()
  total_time = round(end_time - start_time,2)
  print('-' * 80)
  print(f'Concurrent unit test run ending at {datetime.datetime.now()}')
  print(f'Total elapsed time for testing: {str(datetime.timedelta(seconds = (total_time)))}')
  print('-' * 80)
  print(f'Total number of unit tests run = {len(test_notebook_list)}')
  print(f'{run_pass} passed; {run_fail} failed')
  if run_fail > 0:
     print('\n'.join(['- '+x for x in notebooks_fail]))
  print('-' * 80)
  # Unit test status return
  if run_fail == 0:
    return 'pass'
  else:
    return 'fail'

# COMMAND ----------

class NotebookData:
  '''NotebookData
  This class is responsible for identifying the notebook to submit for parallel running (via path) and
  handling the running of the notebook (via dbutils.notebook.run). There is the option to retry notebooks
  (self.retry) for n number of times (if notebook returns a failed status).
  '''
  def __init__(self, path, timeout, retry=0):
    self.path = path
    self.timeout = timeout
    self.retry = retry

  def submitNotebook(notebook):
    print("Running notebook %s" % notebook.path)
    try:
      return (notebook.path,dbutils.notebook.run(notebook.path, notebook.timeout))
    except Exception:
       if notebook.retry < 1:
        return (notebook.path,'fail')
    print("Retrying notebook %s" % notebook.path)
    notebook.retry = notebook.retry - 1
    submitNotebook(notebook)

def parallelNotebooks(notebooks, numInParallel):
  '''parallelNotebooks
  Function is responsible for launching the parallel notebook run jobs, where notebooks are wrapped in the
  NotebookData class. The number of parallel notebook jobs can be user specified.
  Warning: If you create too many notebooks in parallel the driver may crash when you submit all of the
  jobs at once.

  Args:
    notebooks (List[NotebookData]): A list of notebooks defined using the NotebookData class.
    numInParallel (int):            Number of notebook jobs to run in parallel.
  '''
  with ThreadPoolExecutor(max_workers=numInParallel) as ec:
    return [ec.submit(NotebookData.submitNotebook, notebook) for notebook in notebooks]

def run_unit_tests_parallel(test_notebook_list: List[str], parent_path: str = 'tests/unit_tests', num_jobs: int = 2):
  '''run_unit_tests_parallel
  Version of run_unit_tests that allows for parallel running of unit test notebooks. The number of parallel jobs is specified by the
  num_jobs paramters.
  Note: The default number of parallel jobs is set to 2. Be cautious of increasing the number of jobs (e.g. 8) as this can cause the
  parallel job handler to crash. Optimum range is 2 to 4 parallel jobs.

  Args:
    test_notebook_list (List[str]): List of unit test notebooks (in parent_path) to run.
    parent_path (str, optional): The folder containing the unit test notebooks. Defaults to tests/unit_tests.
    num_jobs (int, optional): Number of parallel jobs to run. Defaults to 2.
  '''
  # Initialise run
  print('-' * 80)
  print(f'Parallel unit test run starting at {datetime.datetime.now()}')
  print('-' * 80)
  start_time = time.time()
  # Create parallel notebook objects
  notebooks = [NotebookData(f'../{parent_path}/{x}', 0) for x in test_notebook_list]
  # Run notebooks in parallel
  res = parallelNotebooks(notebooks, num_jobs)
  result = [i.result(timeout=3600) for i in res]
  results_total = len(result)
  results_passed = Counter(elim[1] for elim in result)['pass']
  results_failed = results_total - results_passed
  notebooks_failed = [elim[0] for elim in result if elim[1] != 'pass']
  # Update console
  print(f'Total number of unit tests run = {len(result)}')
  print(f"Unit tests: passed = {results_passed}; failed = {results_failed}")
  if results_failed > 0:
    print('Failed unit tests:')
    for notebook in notebooks_failed:
      print(f'- {notebook}')
  print('-' * 80)
  end_time = time.time()
  total_time = round(end_time - start_time,2)
  print(f'Parallel unit test run ending at {datetime.datetime.now()}')
  print(f'Total elapsed time for testing: {str(datetime.timedelta(seconds = (total_time)))}')
  print('-' * 80)
  # Unit test status return
  if results_failed == 0:
    return 'pass'
  else:
    return 'fail'
