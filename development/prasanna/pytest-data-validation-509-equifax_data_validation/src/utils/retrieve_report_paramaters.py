import pandas as pd


class ReportTestParametersRetrieve:
    def retrieve_widget_test_parameters(self, report_name, report_type=None, application_name=None):
        report_path = "config/report_params/jira_reports.csv"
        if report_type:
            report_path = "config/report_params/" + report_type + "_reports.csv"

        df_params = pd.read_csv(report_path)  ## report parameters list
        df_params_req = df_params[df_params["report"] == report_name]
        df_params_req = df_params_req.dropna(axis=1, how="all")
        df_params_req = df_params_req.drop(["report"], axis=1)
        df_params_req = df_params_req.transpose()
        df_params_req = df_params_req.fillna("")

        # setting first row as column headers
        new_header = df_params_req.iloc[0]  # grab the first row for the header
        df_params_req = df_params_req[1:]  # take the data less the header row
        df_params_req.columns = new_header  # set the header row as

        df_params_list = df_params_req.values.tolist()
        if application_name:
            df_params_list = [tuple([application_name] + lst) for lst in df_params_list]
        else:
            df_params_list = [tuple(lst) for lst in df_params_list]

        return df_params_list

    def retrieve_report_filter_values(self, report_name, field_name, report_type=None, application_name=None):
        report_path = "config/report_params/jira_reports.csv"
        if report_type:
            report_path = "config/report_params/" + report_type + "_reports.csv"

        df_params = pd.read_csv(report_path)  ## report parameters list
        df_params_req = df_params[(df_params["report"] == report_name) & (df_params["filters"] == field_name)]
        df_params_req = df_params_req.dropna(axis=1, how="all")
        df_params_req = df_params_req.drop(["report", "filters"], axis=1)

        df_params_list = df_params_req.values.tolist()
        if df_params_list:
            df_params_list = set(lst for lst in df_params_list[0])
            df_params_list = list(df_params_list)

        if application_name:
            df_params_list = [tuple([application_name, lst]) for lst in df_params_list]

        return df_params_list
