# Databricks notebook source
# MAGIC %run ./pipeline_util

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

class CreateLoggerOutputStage(PipelineStage):
  """
    CreateLoggerOutputStage:

      SUMMARY:
        Main purpose is to take the logger class, transform the results dictionary into a dataframe
        and subsequently a pipeline asset, and return as a pipeline asset to be written to the database

  """
  def __init__(self, logger_output: str):
      self.logger_output: str = logger_output
      self._data_holder: Dict[str, DataFrame] = {}
      super(CreateLoggerOutputStage, self).__init__(set(), {self.logger_output})


  def _run(self, context, log):

      # dictionary collected from the logger class
      log_dictionary = log._get_dict()

      # final information about the pipeline is added
      log_dictionary['Full Pipeline']['pipeline_end_date'] =  datetime.now().strftime("%m/%d/%Y %H:%M:%S")
      log_dictionary['Full Pipeline']['end_time'] = time.time()
      log_dictionary['Full Pipeline']['total_time'] = log_dictionary['Full Pipeline']['end_time'] - log_dictionary['Full Pipeline']['start_time']

      #create a DataFrame from the dictionary
      self._data_holder['log'] = self._generate_logger_dataframe(log_dictionary)

      return {
        self.logger_output:  PipelineAsset(self.logger_output, context, df = self._data_holder['log'], cache = True)
      }


  def _generate_logger_dataframe(self, log_dict):
      tmp_arr = []

      # loops through all dictionary items and creates a row from the key and inner values in the nested dictionary
      for k,v in log_dict.items():
        for inner_k, inner_v in v.items():
          tmp_arr.append(Row(stage=k, criteria=inner_k, value=str(inner_v)))
          df = spark.createDataFrame(data=tmp_arr, schema=['stage','criteria', 'value'])
          # adds a category field which can be used for filtering
          df = df.withColumn('category',
                             F.when(F.col('criteria').contains('time'), 'timing')
                             .when(F.col('stage') == 'Full Pipeline', 'pipeline_stats')
                             .otherwise('counts'))

      return df