#!/bin/bash
set -x

declare -r TEST_SUITE
declare -r ENV
declare -r TENANT
declare -r USER
declare -r TEST_PATH

cd src

#!/bin/sh
if [ "$TEST_SUITE" ]; then
  python3 -m pytest -q -s  -m $TEST_SUITE -n 10 --html=../result/report.html --css assets/style_custom.css --alluredir=../result/allure-report/ --junitxml=../result/result.xml
else
    python3 -m pytest -q $TEST_PATH --html=../result/report.html --css assets/style_custom.css --alluredir=../result/allure-report/ --junitxml=../result/result.xml
fi

if [ $TEST_SUITE="dpstestcases" ]
then
  echo "result analysis"
  python3 ../tools/data_validation_results_analysis.py
else
  echo "test execution completed"
fi
