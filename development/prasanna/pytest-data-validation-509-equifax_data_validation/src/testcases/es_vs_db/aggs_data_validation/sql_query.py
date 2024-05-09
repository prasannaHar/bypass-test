import logging
from datetime import date, timedelta

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
class SqlQuery:
    def __init__(self):
        # self.d= date.today()\
            #.strftime("%Y-%m-%d")
        self.lt=str(date.today()-timedelta(days=5))
        self.gt=str(date.today()-timedelta(days=6))
        self.epoch_lt = str((date.today() - timedelta(days=5)).strftime('%s'))
        self.epoch_gt=str((date.today()-timedelta(days=6)).strftime('%s'))
        LOG.info("time period is : {} and {}".format(self.gt,self.lt))



        # self.d1='2023-01-22'

    def query_scm_commit_pullrequest_mappings(self, tenant_name, integration_id):

        query = "SELECT a.*,b.integration_id FROM " + tenant_name + ".scm_commit_pullrequest_mappings as a \
                join " + tenant_name + ".scm_pullrequests as b \
                on \
                a.scm_pullrequest_id=b.id \
                join " + tenant_name + ".scm_commits c \
                on \
                a.scm_commit_id=c.id \
                where b.integration_id='" + integration_id + "' and c.integration_id='" + integration_id + "'and b.pr_updated_at<'" + self.lt + "' and b.pr_updated_at>'" + self.gt + "'"

        return query

    def star_query(self, table_name, integration_id=None, scm_integration_id=None, scm_integ_id=None):


        try:
            if integration_id is not None:
                if 'scm_commits' in table_name:
                    query = "Select * from " + table_name + " where integration_id='" + integration_id + "' and committed_at<'" + self.lt + "' and committed_at>'" + self.gt + "'"
                elif 'scm_issues' in table_name:
                    query = "Select * from " + table_name + " where integration_id='" + integration_id + "' and issue_updated_at<'" + self.lt + "' and issue_updated_at>'" + self.gt + "'"
                elif 'scm_pullrequests' in table_name:
                    query = "Select * from " + table_name + " where integration_id='" + integration_id + "' and pr_updated_at<'" + self.lt + "' and pr_updated_at>'" + self.gt + "'"


                elif "scm_files" in table_name:
                    query="Select * from " + table_name + " where integration_id='" + integration_id + "' and created_at>'"+self.epoch_gt+"' and created_at<'"+self.epoch_lt+"'"

                elif "scm_tags" in table_name:
                    query="Select a.* from " + table_name + " a join " + table_name.split(".")[0] + ".scm_commits b \
                                                 on a.integration_id = b.integration_id and a.commit_sha =b.commit_sha \
                                                 where a.integration_id='" + integration_id + "' and committed_at<'" + self.lt + "' and committed_at>'" + self.gt + "'"

                else:
                    query = "Select * from " + table_name + " where integration_id='" + integration_id + "'"



            elif scm_integ_id is not None:
                if "scm_commit_jira_mappings" in table_name:
                    query = "Select a.* from " + table_name + " a join " + table_name.split(".")[0] + ".scm_commits b \
                                                 on a.scm_integ_id = b.integration_id and a.commit_sha =b.commit_sha \
                                                 where a.scm_integ_id='" + scm_integ_id + "' and committed_at<'" + self.lt + "' and committed_at>'" + self.gt + "'"


                else:
                    query = "Select * from " + table_name + " where scm_integ_id='" + scm_integ_id + "'"

            else:
                if "scm_pullrequests_workitem_mappings" in table_name:
                    query = "select a.* from " + table_name + " a  join " + table_name.split(".")[0] + ".scm_pullrequests b on         \
                                                  a.scm_integration_id=b.integration_id  and a.pr_id = b.number  \
                                                  where  b.integration_id ='" + scm_integration_id + "' and pr_updated_at<'" + self.lt + "' and pr_updated_at>'" + self.gt + "' order by a.pr_id "

                elif "scm_pullrequests_jira_mappings" in table_name:
                    query = "select a.* from " + table_name + " a  join " + table_name.split(".")[0] + ".scm_pullrequests b on         \
                                                  a.scm_integration_id=b.integration_id  and a.pr_id = b.number  \
                                                  where  b.integration_id ='" + scm_integration_id + "' and pr_updated_at<'" + self.lt + "' and pr_updated_at>'" + self.gt + "' order by a.pr_id "

                elif "scm_commit_workitem_mappings" in table_name:
                    query = "Select a.* from " + table_name + " a join " + table_name.split(".")[0] + ".scm_commits b \
                                                   on a.scm_integration_id = b.integration_id and a.commit_sha =b.commit_sha \
                                                   where a.scm_integration_id='" + scm_integration_id + "' and committed_at<'" + self.lt + "' and committed_at>'" + self.gt + "'"
                else:
                    query = "Select * from " + table_name + " where scm_integration_id='" + scm_integration_id + "'"

            return query

        except Exception as ex:
            LOG.info("Exception while exceuting the query ---:{}".format(ex))



    def scm_file_commits_query(self, integration_id, tenant_name):
        query = "select a. * from " + tenant_name + ".scm_file_commits a join " + tenant_name + ".scm_commits \
        b \
        on \
        a.commit_sha = b.commit_sha \
        where \
        b.integration_id='" + integration_id + "'and a.committed_at<'" + self.lt + "' and a.committed_at>'" + self.gt + "' order by a.committed_at desc"

        return query

    def scm_pullrequest_reviews_query(self, integration_id, tenant_name):
        query = "select a.* from " + tenant_name + ".scm_pullrequest_reviews a \
        join " + tenant_name + ".scm_pullrequests b \
        on  \
        a.pr_id=b.id \
        where \
        b.integration_id ='" + integration_id + "' and a.reviewed_at<'" + self.lt + "' and "+" a.reviewed_at>'" + self.gt + "' order by a.reviewed_at desc "
        return query

    def scm_pullrequest_labels_query(self, integration_id, tenant_name):
        query = "select a.* from " + tenant_name + ".scm_pullrequest_labels a \
        join " + tenant_name + ".scm_pullrequests b \
        on \
        a.scm_pullrequest_id=b.id \
        where  \
        b.integration_id ='" + integration_id + "' and a.label_added_at<'" + self.lt + "' and a.label_added_at>'"+self.gt+"' order by a.label_added_at desc"

        return query

    def scm_github_project_cols_query(self, integration_id, tenant_name):
        query = "select a.* from " + tenant_name + ".github_project_columns a \
        join " + tenant_name + ".github_projects b \
        on a.project_id=b.id \
        where b.integration_id='" + integration_id + "'"
        return query

    def scm_gitrepositories_query(self, integration_id, table_name):
        query = "Select * from " + table_name + " where integrationid='" + integration_id + "'"
        return query

    def github_cards_query(self, integration_id, tenant_name):
        query = "select a.* from " + tenant_name + ".github_cards a \
        join \
        " + tenant_name + ".github_project_columns b \
        on \
        a.current_column_id =b.id \
        join " + tenant_name + ".github_projects c \
        on b.project_id=c.id \
        where c.integration_id='" + integration_id + "'"

        return query

    def scm_tag_query(self, tenant_name, integration_id):
        query = "Select * \
        from " + tenant_name + ".scm_tags a\
        join\
        " + tenant_name + ".scm_commits\
        b\
        on\
        a.commit_sha = b.commit_sha and a.integration_id = b.integration_id\
        where\
        b.integration_id = '" + integration_id + "' and b.committed_at < '" + self.lt + "' and b.committed_at > '"+self.gt+"'"

        return query

    def scm_commit_jira_workitem_mappings_query(self, tenant_name, integration_id, table_name):
        if "jira" in table_name:
            query = "select a.* from " + tenant_name + "." + table_name + " a join\
            " + tenant_name + ".scm_commits b on a.commit_sha=b.commit_sha\
            and a.scm_integ_id = b.integration_id \
            where b.integration_id='" + integration_id + "' and b.committed_at < '" + self.lt + "' and b.committed_at > '"+self.gt+"'"

        else:
            query = "select a.* from " + tenant_name + "." + table_name + " a join\
                    " + tenant_name + ".scm_commits b on a.commit_sha=b.commit_sha\
                    and a.scm_integration_id = b.integration_id \
                    where b.integration_id='" + integration_id + "' and b.committed_at < '" + self.lt + "' and b.committed_at > '"+self.gt+"'"

        return query

    def scm_pullrequests_jira_workitem_mappings(self, tenant_name, integration_id, table_name):
        query = "Select a.* from " + tenant_name + "." + table_name + " a join\
        " + tenant_name + ".scm_pullrequests b on a.scm_integration_id=b.integration_id and a.pr_id =b.number\
        where b.integration_id='" + integration_id + "' and b.pr_updated_at<'" + self.lt + "' and b.pr_updated_at>'" + self.gt + "'"
        return query

    def scm_commit_pullrequest_mappings_except_query(self, tenant_name, integration_id1, integration_id2):
        query = "(select c.commit_sha from (select b.* from " + tenant_name + ".scm_pullrequests a\
                join\
                " + tenant_name + ".scm_commits b on a.integration_id =b.integration_id\
                where b.commit_sha  = any(a.commit_shas) and a.integration_id='" + integration_id1 + "' ) as c join \
                " + tenant_name + ".scm_commit_pullrequest_mappings d on\
                c.id =d.scm_commit_id)\
                Except\
                (select c.commit_sha from (select b.* from " + tenant_name + ".scm_pullrequests a\
                join\
                " + tenant_name + ".scm_commits b on a.integration_id =b.integration_id\
                where b.commit_sha  = any(a.commit_shas) and a.integration_id='" + integration_id2 + "' ) as c join \
                " + tenant_name + ".scm_commit_pullrequest_mappings d on\
                c.id =d.scm_commit_id)"

        return query

    def cicd_jobs_query(self, tenant_name, integration_id, time_range=None):
        query = "SELECT A.* " \
                f"FROM {tenant_name}.CICD_JOBS A " \
                f"JOIN {tenant_name}.CICD_INSTANCES B " \
                f"ON A.cicd_instance_id = B.id " \
                f"AND B.integration_id = '{integration_id}' " \

        if time_range:
            query = query + f"AND A.UPDATED_AT >= '{time_range['$gt']}' " \
                f"AND A.UPDATED_AT <= '{time_range['$lt']}' "

        query = query + "ORDER BY A.cicd_instance_id"

        return query

    def cicd_job_runs_query(self, tenant_name, integration_id, time_range=None):
        query = "SELECT B.PROJECT_NAME, B.JOB_NAME, A.* " \
                f"FROM {tenant_name}.CICD_JOB_RUNS A " \
                f"JOIN {tenant_name}.CICD_JOBS B " \
                "ON A.CICD_JOB_ID = B.ID " \
                f"JOIN {tenant_name}.CICD_INSTANCES C " \
                "ON B.CICD_INSTANCE_ID = C.ID " \
                f"AND C.INTEGRATION_ID = '{integration_id}' "

        if time_range:
                query = query + f"AND A.END_TIME >= '{time_range['$gt']}' " \
                    f"AND A.END_TIME <= '{time_range['$lt']}' "

        query = query + "ORDER BY B.PROJECT_NAME, B.JOB_NAME "

        return query

    def cicd_job_run_stages_query(self, tenant_name, integration_id, time_range=None):
        query = "SELECT C.PROJECT_NAME, C.JOB_NAME, B.JOB_RUN_NUMBER, A.* " \
                f"FROM {tenant_name}.CICD_JOB_RUN_STAGES A " \
                f"JOIN {tenant_name}.CICD_JOB_RUNS B ON A.CICD_JOB_RUN_ID = B.ID " \
                f"JOIN {tenant_name}.CICD_JOBS C ON B.CICD_JOB_ID = C.ID " \
                f"JOIN {tenant_name}.CICD_INSTANCES D ON C.CICD_INSTANCE_ID = D.ID " \
                f"AND D.INTEGRATION_ID = '{integration_id}' "
        if time_range:
                query = query + f"AND B.END_TIME >= '{time_range['$gt']}' " \
                    f"AND B.END_TIME <= '{time_range['$lt']}' "

        query = query + "ORDER BY B.JOB_RUN_NUMBER, A.STAGE_ID"

        return query

    def cicd_job_run_stage_steps_query(self, tenant_name, integration_id, time_range=None):
        query = "SELECT D.PROJECT_NAME, " \
                    "D.JOB_NAME, " \
                    "C.JOB_RUN_NUMBER, " \
                    "B.stage_id, " \
                    "A.* " \
                f"FROM {tenant_name}.CICD_JOB_RUN_STAGE_STEPS A " \
                f"JOIN {tenant_name}.CICD_JOB_RUN_STAGES B ON A.CICD_JOB_RUN_STAGE_ID = B.ID " \
                f"JOIN {tenant_name}.CICD_JOB_RUNS C ON B.CICD_JOB_RUN_ID = C.ID " \
                f"JOIN {tenant_name}.CICD_JOBS D ON C.CICD_JOB_ID = D.ID " \
                f"JOIN {tenant_name}.CICD_INSTANCES E ON D.CICD_INSTANCE_ID = E.ID " \
                f"AND E.INTEGRATION_ID = '{integration_id}' "
        if time_range:
                query = query + f"AND C.END_TIME >= '{time_range['$gt']}' " \
                    f"AND C.END_TIME <= '{time_range['$lt']}' " \

        query = query + "ORDER BY C.JOB_RUN_NUMBER, A.STEP_ID"

        return query

    def cicd_job_run_params_query(self, tenant_name, integration_id, time_range=None):
        query = "SELECT C.PROJECT_NAME, " \
                "    C.JOB_NAME," \
                "    B.JOB_RUN_NUMBER, " \
                "    A.* " \
                f"FROM {tenant_name}.CICD_JOB_RUN_PARAMS A " \
                f"JOIN {tenant_name}.CICD_JOB_RUNS B ON A.CICD_JOB_RUN_ID = B.ID " \
                f"JOIN {tenant_name}.CICD_JOBS C ON B.CICD_JOB_ID = C.ID " \
                f"JOIN {tenant_name}.CICD_INSTANCES D ON C.CICD_INSTANCE_ID = D.ID " \
                f"AND D.INTEGRATION_ID = '{integration_id}' "

        if time_range:
                query = query + f"AND B.END_TIME >= '{time_range['$gt']}' " \
                    f"AND B.END_TIME <= '{time_range['$lt']}' " \

        query = query + "ORDER BY B.END_TIME"

        return query

    def cicd_scm_mapping_query(self, tenant_name, integration_id, time_range=None):
        query = "SELECT D.PROJECT_NAME, " \
                "   D.JOB_NAME, " \
                "   C.JOB_RUN_NUMBER, " \
                "   A.* " \
                "FROM GITLABETL.CICD_SCM_MAPPING A " \
                f"JOIN {tenant_name}.SCM_COMMITS B ON A.COMMIT_ID = B.ID " \
                f"JOIN {tenant_name}.CICD_JOB_RUNS C ON A.CICD_JOB_RUN_ID = C.ID " \
                f"JOIN {tenant_name}.CICD_JOBS D ON C.CICD_JOB_ID = D.ID " \
                f"JOIN {tenant_name}.CICD_INSTANCES E ON D.CICD_INSTANCE_ID = E.ID " \
                f"AND E.INTEGRATION_ID = '{integration_id}' "
        if time_range:
                query = query + f"AND C.END_TIME >= '{time_range['$gt']}' " \
                    f"AND C.END_TIME <= '{time_range['$lt']}' " \

        query = query + "ORDER BY C.END_TIME"

        return query