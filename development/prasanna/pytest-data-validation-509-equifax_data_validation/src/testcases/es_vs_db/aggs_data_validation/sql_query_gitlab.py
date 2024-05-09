import logging
from datetime import date, timedelta

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
class SqlQueryGitlab:

    def query_scm_commit_pullrequest_mappings(self, tenant_name, integration_id):

        query = "SELECT a.*,b.integration_id FROM " + tenant_name + ".scm_commit_pullrequest_mappings as a \
                join " + tenant_name + ".scm_pullrequests as b \
                on \
                a.scm_pullrequest_id=b.id \
                join " + tenant_name + ".scm_commits c \
                on \
                a.scm_commit_id=c.id \
                where b.integration_id='" + integration_id + "' and c.integration_id='" + integration_id + "'"

        return query

    def star_query(self, table_name, integration_id=None, scm_integration_id=None, scm_integ_id=None):


        try:
            if integration_id is not None:
                if 'scm_commits' in table_name:
                    query = "Select * from " + table_name + " where integration_id='" + integration_id + "'"
                elif 'scm_issues' in table_name:
                    query = "Select * from " + table_name + " where integration_id='" + integration_id + "'"
                elif 'scm_pullrequests' in table_name:
                    query = "Select * from " + table_name + " where integration_id='" + integration_id + "'"


                elif "scm_files" in table_name:
                    query="Select * from " + table_name + " where integration_id='" + integration_id + "'"

                elif "scm_tags" in table_name:
                    query="Select a.* from " + table_name + " a join " + table_name.split(".")[0] + ".scm_commits b \
                                                 on a.integration_id = b.integration_id and a.commit_sha =b.commit_sha \
                                                 where a.integration_id='" + integration_id + "'"

                else:
                    query = "Select * from " + table_name + " where integration_id='" + integration_id + "'"



            elif scm_integ_id is not None:
                if "scm_commit_jira_mappings" in table_name:
                    query = "Select a.* from " + table_name + " a join " + table_name.split(".")[0] + ".scm_commits b \
                                                 on a.scm_integ_id = b.integration_id and a.commit_sha =b.commit_sha \
                                                 where a.scm_integ_id='" + scm_integ_id + "'"


                else:
                    query = "Select * from " + table_name + " where scm_integ_id='" + scm_integ_id + "'"

            else:
                if "scm_pullrequests_workitem_mappings" in table_name:
                    query = "select a.* from " + table_name + " a  join " + table_name.split(".")[0] + ".scm_pullrequests b on         \
                                                  a.scm_integration_id=b.integration_id  and a.pr_id = b.number  \
                                                  where  b.integration_id ='" + scm_integration_id + "' order by a.pr_id "

                elif "scm_pullrequests_jira_mappings" in table_name:
                    query = "select a.* from " + table_name + " a  join " + table_name.split(".")[0] + ".scm_pullrequests b on         \
                                                  a.scm_integration_id=b.integration_id  and a.pr_id = b.number  \
                                                  where  b.integration_id ='" + scm_integration_id + "' order by a.pr_id "

                elif "scm_commit_workitem_mappings" in table_name:
                    query = "Select a.* from " + table_name + " a join " + table_name.split(".")[0] + ".scm_commits b \
                                                   on a.scm_integration_id = b.integration_id and a.commit_sha =b.commit_sha \
                                                   where a.scm_integration_id='" + scm_integration_id + "'"
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
        b.integration_id='" + integration_id + "' order by a.committed_at desc"

        return query

    def scm_pullrequest_reviews_query(self, integration_id, tenant_name):
        query = "select a.* from " + tenant_name + ".scm_pullrequest_reviews a \
        join " + tenant_name + ".scm_pullrequests b \
        on  \
        a.pr_id=b.id \
        where \
        b.integration_id ='" + integration_id + "' order by a.reviewed_at desc "
        return query

    def scm_pullrequest_labels_query(self, integration_id, tenant_name):
        query = "select a.* from " + tenant_name + ".scm_pullrequest_labels a \
        join " + tenant_name + ".scm_pullrequests b \
        on \
        a.scm_pullrequest_id=b.id \
        where  \
        b.integration_id ='" + integration_id + "' order by a.label_added_at desc"

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
        b.integration_id = '" + integration_id + "'"

        return query

    def scm_commit_jira_workitem_mappings_query(self, tenant_name, integration_id, table_name):
        if "jira" in table_name:
            query = "select a.* from " + tenant_name + "." + table_name + " a join\
            " + tenant_name + ".scm_commits b on a.commit_sha=b.commit_sha\
            and a.scm_integ_id = b.integration_id \
            where b.integration_id='" + integration_id + "'"

        else:
            query = "select a.* from " + tenant_name + "." + table_name + " a join\
                    " + tenant_name + ".scm_commits b on a.commit_sha=b.commit_sha\
                    and a.scm_integration_id = b.integration_id \
                    where b.integration_id='" + integration_id + "'"

        return query

    def scm_pullrequests_jira_workitem_mappings(self, tenant_name, integration_id, table_name):
        query = "Select a.* from " + tenant_name + "." + table_name + " a join\
        " + tenant_name + ".scm_pullrequests b on a.scm_integration_id=b.integration_id and a.pr_id =b.number\
        where b.integration_id='" + integration_id + "'"
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
