import pandas as pd

# test script template- need to use in exact format with empty lines
# column and data mapping -- 
#    <column name>:
#    <column data line1>
#    <column data line2>
#    <column data line3> etc.. 
# leave one empty line seperator between one column to another column
# leave 2 empty lines as seprator between once test case to another test case
# Note: dont add any extra lines at the starting or end of test script

'''
Name: 
SCM PRs Report widget v/s drill-down data consistency with across:|across|;filters: |if "filters" is not "none"| |filters|, |endif|
|if "filter 2" is not "none"| |filter 2|, |endif|
|if "filter 3" is not "none"| |filter 3| |endif|.

Precondition:
Existing workspace(with SCM integration attached) with valid OU categories and dashboard attached to it

Status:
Active

Objective:
verify widget v/s drill-down data consistency 

Test Steps:
1. Create SCM PRs report
2. Set across to |across| |if "interval" is not "none"| interval:|interval| |endif|
3. Apply filters |if "filters" is not "none"| |filters|, |endif|
|if "filter 2" is not "none"| |filter 2|, |endif|
|if "filter 3" is not "none"| |filter 3| |endif|,|time range filter|
4. set sort xaxis to |sort_xaxis|
5. widget data should be consistent with the drill-down data


'''



def split_text_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.read().split('\n\n\n')
    return lines

# Example usage
file_path = '../tools/input.txt'
lines_list = split_text_file(file_path)

tcs = []
columns = []
columns_not_exists_flag = True

for eachtc in lines_list:
    tc_parts = eachtc.split("\n\n")
    temp_tc = []
    for eachtcpart in tc_parts:
        if len((eachtcpart.split(":\n"))) >= 2:
            temp_tc.append( (eachtcpart.split(":\n"))[1] )
            if columns_not_exists_flag:
                columns.append((eachtcpart.split(":\n"))[0])
    tcs.append(temp_tc)
    columns_not_exists_flag = False

tcs_DF = pd.DataFrame(tcs, columns=columns)
tcs_DF.to_csv('output.csv', index=False)
print("test generated successfully: result file output.csv")
