

Execution steps - 

    1. Terminal > navigate to repo directory > execute below commands
    2. Command for installing the packages 
        pip3 install -r requriment.txt
    3. Export Variable need to run in Terminal
        ```export USER= <username@propelo.ai>
            export PASSWORD=<password>
            export ENV=<env name eg: prod>
            export TENANT=<tenent name eg: foo>
            export BASE_URL=<baseurl- eg: https://api.levelops.io/v1/>
            export DBUSER=<DB User eg: postgres>
            export HOST=<DB host>
            export DATABASE=<DB database>
            export DB_PASSWORD=<DB password>
            export ENV_FILE= "testui1_automation.json"```
        
    4.  Python3 -m pytest -q  --html=report.html --self-contained-html testcases/<Test-suite Path>

        eg: python3 -m pytest -q  --html=report.html --self-contained-html testcases/Hygiene_customer_tenent/test_copy_pandas.py
        
    5. Usescase wise test cmd:
        Python3 -m pytest -q  --html=report.html --self-contained-html testcases/<Test-suite Path> -k <Usecase name>
        eg: python3 -m pytest -q  --html=report.html --self-contained-html testcases/Hygiene_customer_tenent/test_copy_pandas.py -k test_all_tickets

    6. Marker wise test cmd:
        Python3 -m pytest -q -m <Mark Name> --html=report.html --self-contained-html
        eg:python3 -m pytest -q  -m propelslist --html=report.html --self-contained-html testcases/Hygiene_customer_tenent/test_copy_pandas.py

        
[//]: # (        test-suits/test_list.sh - for running the testsuite)

[//]: # ()
[//]: # (        python3 -m pytest -q -s - test run )

[//]: # (        python3 -m pytest -q -s -n 10 - parallel execution)

[//]: # (        python3 -m pytest -q -s --env testui1 --tenant foo -- testui1 execution)

[//]: # ()
[//]: # ()
[//]: # (        python3 -m pytest -q -s --env testui1 --tenant foo -n 10 --html=report.html --capture sys -rF -rP -> html report execution )

[//]: # ()
[//]: # (        python3 -m pytest -q -s --env testui1 --tenant foo -n 10 --html=report.html --capture sys -rF -rP --css assets/style_custom.css -> html report execution with custom css)

[//]: # ()


QA Automation Coverage:

    https://levelops.atlassian.net/wiki/spaces/QAL/pages/1821769802/QA+Automation+Coverage


-- help 

PIP installation:

    python3 get-pip.py

    pip3 install <library-name>

    reference - https://phoenixnap.com/kb/install-pip-mac


POSTMAN installation:

    https://formulae.brew.sh/cask/postman



SHELL script execution:

    https://apple.stackexchange.com/questions/20104/how-do-i-execute-command-line-scripts
    https://apple.stackexchange.com/questions/121000/writing-a-shell-script-or-something-similar-that-will-execute-some-commands




Passing pytest arguments from command line - 


    pytest -q -s --app_config_setup_file test.txt  test_param.py

    https://stackoverflow.com/questions/40880259/how-to-pass-arguments-in-pytest-by-command-line




pytest-html library installation:

    https://www.tutorialspoint.com/selenium_webdriver/selenium_webdriver_generating_html_test_reports_in_python.htm#:~:text=To%20generate%20a%20HTML%20report,%3A%20pytest%20%2D%2Dhtml%3Dreport.


