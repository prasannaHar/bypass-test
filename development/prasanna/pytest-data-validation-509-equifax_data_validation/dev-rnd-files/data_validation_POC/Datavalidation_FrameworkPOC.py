




import json

with open('try.json', 'r') as f:
  data = json.load(f)

# Output: {'name': 'Bob', 'languages': ['English', 'French']}
# print(type(data))

data_json = json.dumps(data)

# print(type(data_json))


pull_requests_data = (data["records"][0]["pull_requests"])


import pandas as pd


pull_request_data_df = pd.DataFrame.from_dict(pull_requests_data)



pull_request_data_df = pull_request_data_df.fillna('')

print("Complete Details dF:")
print(pull_request_data_df)


pull_request_data_df = pull_request_data_df.astype(str)


print("Available coulms: ")
print(pull_request_data_df.columns.to_list())


required_records = pull_request_data_df.loc[pull_request_data_df['assignee'].str.contains("ctlo2020", case=False, na=False)]

print("required records DF")
print(required_records)






