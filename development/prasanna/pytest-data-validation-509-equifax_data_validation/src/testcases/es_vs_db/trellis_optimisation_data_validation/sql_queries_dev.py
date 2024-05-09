
class TrellisOptSqlQuery:

    def terllis_optimisation_query_generator_user_reports(self, tenant_name, table_name):
        query = "select org_user_id, dev_productivity_profile_id, report, interval, score \
                    from  {tenant}.{table} order by org_user_id, dev_productivity_profile_id,\
                    interval".format(tenant=tenant_name, table=table_name)
        return query

    def terllis_optimisation_query_generator_org_reports(self, tenant_name, table_name):
        query = "select ou_id, dev_productivity_profile_id, report, interval, score \
            from  {tenant}.{table} order by ou_id, dev_productivity_profile_id, interval".format(
                                                    tenant=tenant_name, table=table_name)
        return query

