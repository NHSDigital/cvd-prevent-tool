## Overview

Folder contains the:

* Unit tests
* Integration test folder - contains integration tests

Naming conventions:

* Unit test notebooks end `_tests.py` (prefixed by the relevant notebook name)
* Integration test notebooks end `_integration_tests.py` (prefixed by the type of integration test)

## Unit Tests

All unit tests are constructed using the `FunctionTestSuite()` defined in `master/src/test_helpers`. An example of unit test construction is given below.

```
%run ../src/test_helpers

suite = FunctionTestSuite()

@suite.add_test
def test_functionName():
  
  df_input = spark.createDataFrame()
  
  df_expected = spark.createDataFrame()
  
  df_actual = functionName(df_input)
  
  assert compare_results(df_actual, df_expected, join_columns = [COL_NAME])
```

The test suite will then build the unit tests and run them consecutively.

## Integration tests:

Integration Tests test each stage of the pipeline using temporary tables. Theses tables are of length `INTEGRATION_TEST_LIMIT` as defined in `master/params/params_util`. These tables are deleted upon completion of a succesful test, or kept upon a test failing.
