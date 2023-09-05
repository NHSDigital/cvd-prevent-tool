# Databricks notebook source
# main_lib.py
#
# Contains helper scripts and functions for running of the main.py notebook (top level of project repo)

# COMMAND ----------

# MAGIC %run ./util

# COMMAND ----------

# Environment Setup
from functools import wraps
import datetime
import time

# COMMAND ----------

# Exception Handling Classes
class MainWidgetNotFound(Exception):
  pass

# COMMAND ----------

# Decorator - Measurement of execution time
def main_run_timer(func):
  @wraps(func)
  def main_run_timer_wrapper(*args, **kwargs):
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    total_time = end_time - start_time
    total_time_f = str(datetime.timedelta(seconds=total_time))
    print(f'Runtime of {total_time_f} (HH:MM:SS)')
    return result
  return main_run_timer_wrapper

# Decorator - Widget Value Exception Handler
def exception_widget(func):
  def inner_function(*args, **kwargs):
    try:
      func(*args, **kwargs)
    except:
      raise MainWidgetNotFound(f'ERROR: Method {func.__name__} cannot find value for widget {args}. Ensure widget is correctly loaded.')

# COMMAND ----------

# Main Suite - Widget Class
class MainSuite():
  '''MainSuite
  Class for the initialisation and setup of the main.py notebook suite. The class can run the pipeline notebooks when
  the run mode (via widget .Run Mode) is selected:
    Selection Mode (_run_switch = -1):    User required to select mode of running for main.py
    Reset Mode (_run_switch = 0):         Notebook widgets and default values reset to class instantisation
    Pre-Merge Mode (_run_switch = 1):     main.py will run unit test and integration tests only
    Post-Merge Mode (_run_switch = 2):    main.py will run unit and integration tests, followed by full pipeline run
    Pipeline Only Mode (_run_switch = 3): main.py will not run test, will run full pipeline only
  '''
  
  ## Class variables - all instances
  _mode_map = {
    'Select mode': {
      'switch': -1,
      'bool_unit': False,
      'bool_integration': False,
      'bool_pipeline': False
    },
    'Pre-Merge': {
      'switch': 1,
      'bool_unit': True,
      'bool_integration': True,
      'bool_pipeline': False
    },
    'Post-Merge': {
      'switch': 2,
      'bool_unit': True,
      'bool_integration': True,
      'bool_pipeline': True
    },
    'Pipeline Only': {
      'switch': 3,
      'bool_unit': False,
      'bool_integration': False,
      'bool_pipeline': True
    },
    'Reset Notebook': {
      'switch': 0,
      'bool_unit': False,
      'bool_integration': False,
      'bool_pipeline': False
    },
  }
  
  ## Class instantisation
  def __new__(cls, *args, **kwargs):
    return super().__new__(cls)
  
  def __init__(self):
    '''__init__
    Initialises the MainSuite class and sets default values for conditional arguments
    '''
    self._run_mode = None
    self._run_pipeline = False
    self._run_unit_tests = False
    self._run_integration_tests = False
    self._loaded_widgets = []
    
  def __repr__(self):
    '''__repr__
    Prints out the class variables of MainSuite when class is called directly
    '''
    return f'''
    Main Suite: Run mode set to {self._run_mode}.
    Run options set:
    \t[{self._run_pipeline}]\tPIPELINE
    \t[{self._run_unit_tests}]\tUNIT TESTS
    \t[{self._run_integration_tests}]\tINTEGRATION TESTS
    '''    

  ## Public Methods
  def cleanNotebook(self):
    '''cleanNotebook
    Removes all Databricks widgets from the notebook environment
    '''
    dbutils.widgets.removeAll()
  
  def setupNotebook(self):
    '''setupNotebook
    Creates the main run mode interaction widget used for controlling main.py and additional widget for starting the 
    main.py run
    '''
    # Remove all widgets
    dbutils.widgets.removeAll()
    # Main control widget: .Run mode
    dbutils.widgets.dropdown('.Run Mode','Select mode',self._mode_map.keys())
    # Update console with setup message
    self._update_console_setup()
    
  def configureNotebook(self, run_mode: str):
    '''configureNotebook
    Method to configure the MainSuite class for subsequent codebase runs. Contains conditional statements for:
      Selection Mode (_run_switch = -1):    User required to select mode of running for main.py
      Reset Mode (_run_switch = 0):         Notebook widgets and default values reset to class instantisation
      Pre-Merge Mode (_run_switch = 1):     main.py will run unit test and integration tests only
      Post-Merge Mode (_run_switch = 2):    main.py will run unit and integration tests, followed by full pipeline run
      Pipeline Only Mode (_run_switch = 3): main.py will not run test, will run full pipeline only
      
    Args
      run_mode (str): Result from dbutils.widgets.get('.Run Mode') when run in main.py. Specifies run mode for MainSuite.
    '''
    # Remove any widgets from previous runs
    self._remove_widgets()
    # Assign run mode from widget and assign to class variables
    self._initialise_configuration(run_mode)
    # Conditional: if any valid run mode is selected, run main initialisation
    if self._run_switch > 0:
      self._load_widgets()
      # Update console with configuration message
      self._update_console_configuration()
    # Conditional: if default user input mode is selected, display prompt 
    elif self._run_switch == -1:
      print('Select run mode for main.py')
    # Conditional: if notebook reset mode is selected, run reset notebook methods and re-initialise .Run Mode widget
    elif self._run_switch == 0:
      self._reset_notebook()
      self.setupNotebook()
    # Error: No valid run modes selected or provided - fail
    else:
      raise ValueError(f'ERROR: Notebook failure: invalid values for run_mode ({self._run_mode}) and/or run_switch ({self._run_switch}). Clear state and re-run notebook')
    
  def runNotebook(self):
    '''runNotebook
    Used after main.py configuration (self.configureNotebook). Runs the main suite as defined by the configuration options.
    '''
    # Conditional on run mode (_run_switch)
    if self._run_switch > 0:
      # Raise exception if run_mode == None
      if self._run_mode == None:
        raise ValueError('ERROR: self.configureNotebook() must be run before running self.runNotebook - no run_mode variable found')
      # Update class variables for run_mode configuration
      self._updateConfiguration()
      # Update Console
      print(f'Starting main.py run mode ({self._run_mode}) at {datetime.datetime.now()}')
      ## PRE-MERGE OR POST-MERGE MODE
      if (self._run_switch == 1) or (self._run_switch == 2):
        # Main Notebook Running
        self._main_run_unit_tests()
        self._main_run_integration_tests()
      ## POST-MERGE OR PIPELINE MODE
      if (self._run_switch == 2) or (self._run_switch == 3):
        # Main Notebook Running
        self._main_run_pipeline()
      ## Update Console
      print(f'Finished main.py run ({self._run_mode}) at {datetime.datetime.now()}')
    # Conditional: Run mode set to NO (no run of main.py selected)
    elif self._run_switch < 1:
      print("INFO: main.py currently not running")
    # Other: Capture if widget errors
    else:
      raise ValueError(f'ERROR: No .Run Notebook widget found or invalid values for run_mode ({self._run_mode}) and/or run_switch ({self._run_switch}). Reinitialise main.py with self.setupNotebook()')
    
  ## Protected Methods
  # MainSuite Configuration
  def _initialise_configuration(self,run_mode):
    '''_initialise_configuration
    Initialises the configuration variables (_run_mode, _run_pipeline, _run_unit_tests, _run_integration_tests) to the mode
    selected by the .Run Mode widget.
    '''
    self._run_mode = run_mode
    self._run_switch = self._mode_map[run_mode]['switch']
    self._run_pipeline = self._mode_map[run_mode]['bool_pipeline']
    self._run_unit_tests = self._mode_map[run_mode]['bool_unit']
    self._run_integration_tests = self._mode_map[run_mode]['bool_integration']
    
  # MainSuite Notebook Utilities
  def _reset_notebook(self):
    '''_reset_notebook
    Performs a reset of the main.py notebook, removing all specified widgets (excluding .Run Mode) and resets
    __init__ self variables to original defaults
    '''
    # Remove all loaded widgets 
    dbutils.widgets.removeAll()
    # Reset __init__ parameters to default values
    self._run_pipeline = False
    self._run_unit_tests = False
    self._run_integration_tests = False
    
  # MainSuite Runtime Utilities
  @main_run_timer
  def _main_run_unit_tests(self):
    '''_main_run_unit_tests
    Runs the run_unit_tests notebook from ./main_notebooks
    
    Condition(s) for running:
      self._run_unit_tests == True
    '''
    # Update console
    print('-' * 80)
    print('Run Unit Tests')
    print(f'Running unit tests notebook at {datetime.datetime.now()}.')
    start_time = time.time()
    # Run unit tests notebook
    runtime_status = dbutils.notebook.run(
      './main_notebooks/run_unit_tests',
      0,
      {
        'run_mode': self._unit_run
      }
    )
    if runtime_status != 'pass':
      raise RuntimeError('ERROR: run_unit_test notebook has failed. Check notebook job for detailed error message...')
    # Update console
    end_time = time.time()
    total_time = round(end_time - start_time,2)
    print(f'Finished running of unit test notebook at {datetime.datetime.now()}.')
    print(f'Total elapsed time for unit testing: {str(datetime.timedelta(seconds = (total_time)))}')
    print('-' * 80)
    
  @main_run_timer
  def _main_run_integration_tests(self):
    '''_main_run_integration_tests
    Runs the run_integration_tests notebook from ./main_notebooks
    
    Condition(s) for running:
      self._run_integration_tests == True
    '''
    # Update console
    print('-' * 80)
    print('Run Integration Tests')
    print(f'Running integration tests notebook at {datetime.datetime.now()}.')
    start_time = time.time()
    # Run integration tests notebook with specified configuration
    runtime_status = dbutils.notebook.run(
      './main_notebooks/run_integration_tests',
      0,
      {
        'params_path': self._params_path,
        'save_output': self._save_output,
      }
    )
    if runtime_status != 'pass':
      raise RuntimeError('ERROR: run_integration_tests notebook has failed. Check notebook job for detailed error message...')
    # Update console
    end_time = time.time()
    total_time = round(end_time - start_time,2)
    print(f'Finished running of integration test notebook at {datetime.datetime.now()}.')
    print(f'Total elapsed time for integration testing: {str(datetime.timedelta(seconds = (total_time)))}')
    print('-' * 80)
    
  @main_run_timer
  def _main_run_pipeline(self):
    '''_main_run_pipeline
    Runs the run_pipeline notebook from ./main_notebooks
    
    Condition(s) for running:
      self._run_pipeline == True
    '''
    # Update console
    print('-' * 80)
    print('Run Pipeline')
    print(f'Running pipeline notebook at {datetime.datetime.now()}.')
    start_time = time.time()
    # Run the run_pipeline notebook with specified configuration
    runtime_status = dbutils.notebook.run(
      './main_notebooks/run_pipeline',
      0,
      {
        'params_path': self._params_path,
        'git_version': self._git_version,
        'cohort_table': self._cohort_table,
        'prepare_pseudo_assets': self._pseudo_assets,
      }
    )
    if runtime_status != 'pass':
      raise RuntimeError('ERROR: run_pipeline notebook has failed. Check notebook job for detailed error message...')
    # Update console
    end_time = time.time()
    total_time = round(end_time - start_time,2)
    print(f'Finished running of pipeline notebook at {datetime.datetime.now()}.')
    print(f'Total elapsed time for pipeline run: {str(datetime.timedelta(seconds = (total_time)))}')
    print('-' * 80)
    
  def _updateConfiguration(self):
    '''_updateConfiguration
    Updates the configuration after the run mode has been initialised. Used to specify configurable values for required
    notebooks in ./main_notebooks. Updates variables based on the current run_mode.
    Note: This method requires that the widgets from self.configureNotebook have been loaded.
    '''
    # Unit and Integration Tests (run_mode == 1|2)
    if (self._run_switch == 1) or (self._run_switch == 2):
      self._unit_run      = self._assign_widget_value('Unit Test Mode')
      self._params_path   = self._assign_widget_value('Params Path')
      self._save_output   = self._assign_widget_value('Save Integration Tables')
    # Pipeline (run mode == 2|3)
    if (self._run_switch == 2) or (self._run_switch == 3):
      self._params_path   = self._assign_widget_value('Params Path')
      self._git_version   = self._assign_widget_value('Git Version')
      self._cohort_table  = self._assign_widget_value('Cohort Table')
      self._pseudo_assets = self._assign_widget_value('Run Pseudo Stage')
    
  # MainSuite Widget Utilities
  def _assign_widget_value(self,widget_name: str):
    '''_assign_widget_value
    Trys to assign widget value and return for variable assignment. Raises MainWidgetNotFound error if unable
    to load associated widget value
    '''
    try:
      return(dbutils.widgets.get(widget_name))
    except:
      raise MainWidgetNotFound(f'ERROR: unable to parse value from {widget_name}. Check widget selection and widget is lodaded correctly.')
  
  def _load_widgets(self):
    '''_load_widgets
    Conditional loading of databricks widgets. Populates widgets based on bool values of run_integration_tests 
    and run_pipeline. Method saves any loaded widget names to the _loaded_widgets variable, for later removal
    (if required) by _remove_widgets.
    '''
    # If running unit tests, provide dropdown widget for running tests in concurrent or parallel mode
    if self._run_unit_tests:
      dbutils.widgets.dropdown('Unit Test Mode','concurrent',['concurrent','parallel'])
      self._loaded_widgets.extend(['Unit Test Mode'])
    # If running integration tests, provide multiselect option for saving the integration tests
    if self._run_integration_tests:
      dbutils.widgets.dropdown('Save Integration Tables', 'False',['False','True'])
      dbutils.widgets.text('Params Path', 'default')
      self._loaded_widgets.extend(['Save Integration Tables','Params Path'])
    # If running the full pipeline, provide widgets for pipeline parameters (run_pipeline) and multiselect option 
    # for the running of the pseudo preparation stage. Append widget names
    if self._run_pipeline:
      dbutils.widgets.text('Params Path', 'default')
      dbutils.widgets.text('Git Version', '')
      dbutils.widgets.combobox('Cohort Table', '', [find_latest_cohort_table('prevent_tool_collab')])
      dbutils.widgets.dropdown('Run Pseudo Stage', 'False',['False','True'])
      self._loaded_widgets.extend(['Params Path','Git Version','Cohort Table','Run Pseudo Stage'])
    # Reduce set for _loaded_widgets
    self._loaded_widgets = list(set(self._loaded_widgets))
  
  def _remove_widgets(self):
    '''_remove_widgets
    Removes specified widgets, created as part of main.py runtime, from the notebook environment
    '''
    # Loop through loaded widgets and try removal
    # Note: try statement is used incase widget was not loaded, will not error (unable to remove undefined widgets)
    for loaded_widget in self._loaded_widgets:
      try:
        dbutils.widgets.remove(loaded_widget)
      except:
        pass
    # Reset names of loaded widgets
    self._loaded_widgets = []
    
  # MainSuite Console Update Utilities
  def _update_console_setup(self):
    '''_update_console_setup
    Prints out console information depending on the run mode selected (for setup options).
    '''
    print('-' * 80)
    print('''Please select a run mode using the .Run Mode widget:
    - Pre-Merge: Running of the unit test and integration test suite, prior to GitLab merging
    - Post-Merge: Running of the unit test suite, integration test suite, and full pipeline run (post GitLab-to-Databricks merge)
    - Pipeline Only: Running of the full pipeline only (General Running)
    - Reset Mode: Reset notebook widgets and default values reset to class instantisation
    
    Note: if there are errors due to DAE widget issues then please re-run main.py setup command (main.setupNotebook())''')
    print('-' * 80)
    
  def _update_console_configuration(self):
    '''_update_console_configuration
    Prints out console information depending on the run mode selected (for configurable options)
    '''
    print('-' * 80)
    print(f'Run Mode = {self._run_mode}')
    print('')
    # Conditional: Based upon run mode selected
    if self._run_switch > 0:
      print('Please configure main.py run using the following widgets:')
      # All run modes:
      print('- Params Path: Path for the params notebook to use for params bound variables. Defaults to "default" path.')
      # Unit and Integration Tests (run_mode == 1|2)
      if (self._run_switch == 1) or (self._run_switch == 2):
        print('- Save Integration Tables: Save the tables created from the run_integration_test suite (True). Defaults to False.')
        print('- Unit Test Mode: Run unit tests consecutively (Concurrent) or 4 notebooks in parallel (Parallel). Defaults to Concurrent.')
      # Pipeline (run mode == 2|3)
      if (self._run_switch == 2) or (self._run_switch == 3):
        print('- Git Version (required): String, latest git commit hash from the master branch (or custom for development runs). No default, this option must be specified.')
        print('- Cohort Table: Selection (latest cohort table) or string (custom cohort table) of existing cohort table to use. Defaults to blank (generate new cohort table).')
        print('- Run Pseudo Stage: Run the pseudonymisation preparation stage in the pipeline (True). Defaults to False.')
    elif self._run_switch == 0:
      print('INFO: main.py notebook in Reset Mode. Please re-run main.setupNotebook() and select the desired run mode.')
    elif self._run_switch == -1:
      print('INFO: main.py in Selection Mode. Please select the run mode from .Run Mode widget and re-run main.configureNotebook()')
    else:
      raise ValueError('ERROR: Unable to parse widget value/widget is not loaded correctly. Please re-run main.setupNotebook().')
    print('-' * 80)

# COMMAND ----------

main = MainSuite()