
class TrellisOptSqlQuery:

    def terllis_optimisation_query_generator_user_reports(self, tenant_name, table_name):
        query = "select org_user_id, dev_productivity_profile_id, report, interval, score from \
                    {tenant}.{table} where org_user_id in ( select id from {tenant}.org_users as u \
                    where u.versions @> ARRAY(SELECT version FROM {tenant}.org_version_counter \
                    WHERE type ='USER' AND active = true)) and interval = 'LAST_MONTH' \
                    order by org_user_id, dev_productivity_profile_id,\
                    interval".format(tenant=tenant_name, table=table_name)
        return query

    def terllis_optimisation_query_generator_org_reports(self, tenant_name, table_name):
        query = "select ou_id, dev_productivity_profile_id, report, interval, score \
            from  {tenant}.{table} order by ou_id, dev_productivity_profile_id, interval".format(
                                                    tenant=tenant_name, table=table_name)
        return query

