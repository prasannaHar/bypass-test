
class TrellisUserReportSqlQuery:

    def retrieve_jira_display_names(self, tenant, jira_ids):
        sql_query = "select display_name from {tenant}.jira_users".format(tenant=tenant)
        if(len(jira_ids)>0):
            sql_query = sql_query + " where jira_id in ("
            for eachJiraId in jira_ids:
                sql_query = sql_query + "'" + eachJiraId + "', "
            sql_query = sql_query[:len(sql_query)-2]
            sql_query = sql_query + ")"
        return sql_query


    def retrieve_jira_in_progress_status_ids(self, tenant, integration_ids):
        sql_query = "select status_id from {tenant}.jira_status_metadata".format(tenant=tenant)
        if(len(integration_ids)>0):
            sql_query = sql_query + " where integration_id in ("
            for eachJiraId in integration_ids:
                sql_query = sql_query + "'" + eachJiraId + "', "
            sql_query = sql_query[:len(sql_query)-2]
            sql_query = sql_query + ")"
        return sql_query

