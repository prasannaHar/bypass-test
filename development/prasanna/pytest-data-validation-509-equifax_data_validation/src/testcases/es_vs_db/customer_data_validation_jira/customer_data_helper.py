import json
import logging
from copy import deepcopy

import numpy as np
import pandas as pd
import json
import datetime
import inspect
from flatten_json import flatten_json

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class CustomerData:
    def __init__(self):
        self.data_time = datetime.datetime.now()

    def highlight_diff(self, data, color='yellow'):
        attr = 'background-color: {}'.format(color)
        other = data.xs('First', axis='columns', level=-1)
        return pd.DataFrame(np.where(data.ne(other, level=0), attr, ''),
                            index=data.index, columns=data.columns)

    def flatten_json(y):
        out = {}

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(y)
        return out

    def comparing_es_vs_db(self, es_response, db_response, columns=None, add_keys=False, unique_id=None):
        flag = True
        zero_flag = True
        try:
            if len(es_response['records']) == 0:
                LOG.info("ES not having data")
                zero_flag = False
            else:
                es_df = pd.json_normalize(es_response['records'], record_prefix='stacks.')
                if "stacks.keys" in es_df.columns:
                    es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                    es_df = pd.DataFrame(es_df, columns=es_df.columns)
                else:
                    es_df = es_df.sort_values(columns[0])
                    es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                    es_df = pd.DataFrame(es_df, columns=columns)
                if add_keys:
                    es_df['additional_key'] = pd.to_datetime(es_df['additional_key'], format='%d-%M-%Y')

            if len(db_response['records']) == 0:
                LOG.info("DB not having data")
                zero_flag = False
            else:
                db_df = pd.json_normalize(db_response['records'], record_prefix='stacks.')
                if "stacks.keys" in db_df.columns:
                    db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                    db_df = pd.DataFrame(db_df, columns=db_df.columns)
                else:
                    db_df = db_df.sort_values(columns[0])
                    db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                    db_df = pd.DataFrame(db_df, columns=columns)

                if add_keys:
                    db_df['additional_key'] = pd.to_datetime(db_df['additional_key'], format='%d-%M-%Y')
                val1 = pd.merge(db_df, es_df, on=columns,
                                how='outer', indicator=True)

                LOG.info("ES data - {}".format(val1[val1['_merge'] == 'right_only']))
                LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                dd_df = pd.DataFrame([unique_id + "-" + self.data_time.strftime("%m/%d/%Y, %H:%M:%S")])

                dd_df.to_csv(
                    "log_updates/" + str(inspect.stack()[1][3])
                    + '.csv', header=True,
                    index=False, mode='a')

                if len(val1[val1['_merge'] == 'right_only']) != 0:
                    flag = False
                if len(val1[val1['_merge'] == 'left_only']) != 0:
                    (val1[val1['_merge'] == 'left_only']).to_csv(
                        "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                        index=False, mode='a')
                    flag = False
                if (es_response["_metadata"])["total_count"] != (db_response["_metadata"])["total_count"]:
                    LOG.info("ES vs DB count not matching")
                    LOG.info("DB count - {}".format((db_response["_metadata"])["total_count"]))
                    LOG.info("ES count - {}".format((es_response["_metadata"])["total_count"]))
                    flag = False

                if (db_response["_metadata"])["total_count"] == (es_response["_metadata"])["total_count"]:
                    db_df.reset_index(
                        drop=True).compare(es_df.reset_index(drop=True)).to_csv(
                        "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                        index=False, mode='a')

        except Exception as ex:
            LOG.info("Exception {}".format(ex))
            LOG.info("Not executed")
            flag = False
            return flag, zero_flag


        return flag, zero_flag

    def comparing_es_vs_db_ADO(self, es_response, db_response, across_type, columns=None):
        flag = True
        zero_flag = True
        index_count = es_response["count"]
        for value in range(0, int(index_count)):
            try:
                if len(es_response["records"][value][across_type]["records"]) == 0:
                    LOG.info("ES not having data")
                    zero_flag = False
                else:
                    es_df = pd.json_normalize(es_response["records"][0][across_type]["records"],
                                              record_prefix='stacks.')
                    if "stacks.keys" in es_df.columns:
                        es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        es_df = pd.DataFrame(es_df, columns=es_df.columns)
                    else:
                        es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        es_df = pd.DataFrame(es_df, columns=columns)

                if len(db_response["records"][value][across_type]["records"]) == 0:
                    LOG.info("DB not having data")
                    zero_flag = False
                else:
                    db_df = pd.json_normalize(db_response["records"][0][across_type]["records"],
                                              record_prefix='stacks.')
                    if "stacks.keys" in db_df.columns:
                        db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        db_df = pd.DataFrame(db_df, columns=db_df.columns)

                        val1 = pd.merge(db_df, es_df, on=db_df.columns.format(),
                                        how='outer', indicator=True)
                    else:
                        db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        db_df = pd.DataFrame(db_df, columns=columns)

                        val1 = pd.merge(db_df, es_df, on=columns,
                                        how='outer', indicator=True)

                    LOG.info("ES data - {}".format(val1[val1['_merge'] == 'right_only']))
                    LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                    db_df['records_0_full_name'].to_csv(
                        "log_updates/" + str(inspect.stack()[1][3])
                        + '.csv', header=True,
                        index=False, mode='a')

                    if len(val1[val1['_merge'] == 'right_only']) != 0:
                        flag = False
                    if len(val1[val1['_merge'] == 'left_only']) != 0:
                        flag = False
                    if (es_response["_metadata"])["total_count"] != (db_response["_metadata"])["total_count"]:
                        LOG.info("ES vs DB count not matching")
                        LOG.info("DB count - {}".format((db_response["_metadata"])["total_count"]))
                        LOG.info("ES count - {}".format((es_response["_metadata"])["total_count"]))
                        flag = False
                    if (db_response["_metadata"])["total_count"] == (es_response["_metadata"])["total_count"]:
                        db_df.reset_index(
                            drop=True).compare(es_df.reset_index(drop=True)).to_csv(
                            "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                            index=False, mode='a')
            except Exception as ex:
                LOG.info("Not executed")
                zero_flag = False
                return flag, zero_flag
            # comment for priti
            # if not flag:
            #     time_now = datetime.datetime.now()
            #     testname = inspect.stack()[1][3] + (time_now.strftime("%d_%H_%M_%S")+".csv")
            #     LOG.debug("file is generated {}".format(testname))
            #     val1.to_csv(str(testname))
        return flag, zero_flag

    def comparing_es_vs_db_without_prefix(self, es_response, db_response, sort_column_name=None, columns=None,
                                          sort_values_inside_columns=None, sort_values_inside_columns_dict=None,
                                          sort_values_inside_columns_str=None, unique_id=None):
        flag = True
        zero_flag = True
        # breakpoint()
        try:
            if len(es_response['records']) == 0:
                LOG.info("ES not having data")
                zero_flag = False
            else:
                es_df = pd.json_normalize(es_response['records'])

                if sort_column_name:
                    es_df = es_df.sort_values(sort_column_name)
                es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                es_df = pd.DataFrame(es_df, columns=columns)

                if sort_values_inside_columns:
                    for col in es_df.columns:
                        if col in sort_values_inside_columns:
                            es_df[col] = es_df[col].map(lambda x: sorted(x, key=lambda y: int(y)))
                if sort_values_inside_columns_dict:
                    for col in es_df.columns:
                        if col in sort_values_inside_columns_dict:
                            es_df[col] = es_df[col].map(
                                lambda x: sorted(x, key=lambda i: (i['key'], i['additional_key'])))
                if sort_values_inside_columns_str:
                    for col in es_df.columns:
                        if col in sort_values_inside_columns_str:
                            es_df[col] = es_df[col].map(lambda x: sorted(x, key=lambda y: str(y)))
                if columns is not None:
                    es_df = es_df.sort_values(columns[0])
                es_df = es_df.astype(str)

            if len(db_response['records']) == 0:
                LOG.info("DB not having data")
                zero_flag = False
            else:
                db_df = pd.json_normalize(db_response['records'])
                if sort_column_name:
                    db_df = db_df.sort_values(sort_column_name)
                db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                db_df = pd.DataFrame(db_df, columns=columns)
                if sort_values_inside_columns:
                    for col in db_df.columns:
                        if col in sort_values_inside_columns:
                            db_df[col] = db_df[col].map(lambda x: sorted(x, key=lambda y: int(y)))
                if sort_values_inside_columns_dict:
                    for col in db_df.columns:
                        if col in sort_values_inside_columns_dict:
                            db_df[col] = db_df[col].map(
                                lambda x: sorted(x, key=lambda i: (i['key'], i['additional_key'])))
                if sort_values_inside_columns_str:
                    for col in db_df.columns:
                        if col in sort_values_inside_columns_str:
                            db_df[col] = db_df[col].map(lambda x: sorted(x, key=lambda y: str(y)))
                if columns is not None:
                    db_df = db_df.sort_values(columns[0])
                db_df = db_df.astype(str)

                val1 = pd.merge(db_df, es_df, on=columns,
                                how='outer', indicator=True)

                LOG.info("ES data - {}".format(val1[val1['_merge'] == 'right_only']))
                LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                dd_df = pd.DataFrame([unique_id + "-" + self.data_time.strftime("%m/%d/%Y, %H:%M:%S")])

                dd_df.to_csv(
                    "log_updates/" + str(inspect.stack()[1][3])
                    + '.csv', header=True,
                    index=False, mode='a')

                # breakpoint()
                if len(val1[val1['_merge'] == 'right_only']) != 0:
                    flag = False
                if len(val1[val1['_merge'] == 'left_only']) != 0:
                    (val1[val1['_merge'] == 'left_only']).to_csv(
                        "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                        index=False, mode='a')
                    flag = False
                if (es_response["_metadata"])["total_count"] != (db_response["_metadata"])["total_count"]:
                    LOG.info("ES vs DB count not matching")
                    LOG.info("DB count - {}".format((db_response["_metadata"])["total_count"]))
                    LOG.info("ES count - {}".format((es_response["_metadata"])["total_count"]))
                    flag = False

                if (db_response["_metadata"])["total_count"] == (es_response["_metadata"])["total_count"]:
                    db_df.reset_index(
                        drop=True).compare(es_df.reset_index(drop=True)).to_csv(
                        "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                        index=False, mode='a')

        except Exception as ex:
            LOG.info("Exception {}".format(ex))
            LOG.info("Not executed")
            flag = False
            return flag, zero_flag

        # # comment for priti
        # if not flag:
        #     time_now = datetime.datetime.now()
        #     testname = inspect.stack()[1][3] + (time_now.strftime("%d_%H_%M_%S") + ".csv")
        #     LOG.debug("file is generated {}".format(testname))
        #     val1.to_csv(str(testname))

        return flag, zero_flag

    def comparing_es_vs_db_without_prefix_trellis(self, es_response, db_response, sort_column_name=None, columns=None,
                                                  sort_values_inside_columns=None,
                                                  sort_values_inside_columns_dict=None,
                                                  unique_identifier=None):
        flag = True
        zero_flag = True
        tc_name = str(inspect.stack()[1][3])
        LOG.info("tc_name--{}".format(tc_name))
        # import pdb;pdb.set_trace()
        # breakpoint()
        df = pd.DataFrame([unique_identifier])
        df.to_csv(
            "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
            index=False, mode='a')

        try:

            if len(((es_response['records'])[0])['records']) == 0:
                LOG.info("ES not having data")

                zero_flag = False

            else:
                # breakpoint()
                es_df = pd.json_normalize(((es_response['records'])[0])['records'])
                if columns == None:
                    es_columns = es_df.columns.values.tolist()

                if sort_column_name:
                    es_df = es_df.sort_values(sort_column_name)
                es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                es_df = pd.DataFrame(es_df, columns=columns)
                if sort_values_inside_columns:
                    for col in es_df.columns:
                        if col in sort_values_inside_columns:
                            es_df[col] = es_df[col].map(lambda x: sorted(x, key=lambda y: int(y)))
                if sort_values_inside_columns_dict:
                    for col in es_df.columns:
                        if col in sort_values_inside_columns_dict:
                            es_df[col] = es_df[col].map(
                                lambda x: sorted(x, key=lambda i: (i['key'], i['additional_key'])))

                es_df = es_df.round()
                es_df = es_df.astype(str)

            if len(((db_response['records'])[0])['records']) == 0:
                LOG.info("DB not having data")
                zero_flag = False
            else:
                db_df = pd.json_normalize(((db_response['records'])[0])['records'])
                if columns == None:
                    db_columns = db_df.columns.values.tolist()

                if sort_column_name:
                    db_df = db_df.sort_values(sort_column_name)
                db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                db_df = pd.DataFrame(db_df, columns=columns)
                if sort_values_inside_columns:
                    for col in db_df.columns:
                        if col in sort_values_inside_columns:
                            db_df[col] = db_df[col].map(lambda x: sorted(x, key=lambda y: int(y)))
                if sort_values_inside_columns_dict:
                    for col in db_df.columns:
                        if col in sort_values_inside_columns_dict:
                            db_df[col] = db_df[col].map(
                                lambda x: sorted(x, key=lambda i: (i['key'], i['additional_key'])))

                db_df = db_df.round()
                db_df = db_df.astype(str)
                # breakpoint()

                if columns is None:
                    if set(db_columns) == set(es_columns):
                        columns = db_columns
                    elif set(db_columns) != set(es_columns):
                        if set(db_columns).difference(es_columns) == set():
                            col = set(es_columns).difference(db_columns)
                            es_columns = list(col)
                            es_df = es_df.drop(es_columns, axis=1)

                    else:
                        # list = db_columns - es_columns
                        LOG.info("Col in DB not in ES--{}".format(set(db_columns).difference(es_columns)))
                        LOG.info("Col in ES not in DB---{}".format(set(es_columns).difference(db_columns)))
                        LOG.info("es and db columns dont match--{}".format(list))
                        flag = False

                pd.DataFrame(["difference---"]).to_csv("log_updates/" + str(inspect.stack()[1][3]) + '.csv',
                                                       header=True,

                                                       index=False, mode='a')

                if tc_name in ["test_individual_raw_stats_es_vs_db", "test_trellis_dev_es",
                               "test_fixed_intervals_org_list_es_vs_db"]:
                    df_all = pd.concat([db_df.set_index('org_user_id'), es_df.set_index('org_user_id')],
                                       axis='columns', keys=['db', 'es'])
                    df_final = df_all.swaplevel(axis='columns')[db_df.columns[1:]]
                    df_final.style.apply(self.highlight_diff, axis=None)
                    df_final.to_csv(
                        "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                        index=False, mode='a')

                val1 = pd.merge(db_df, es_df, on=columns,
                                how='outer', indicator=True)
                LOG.info("ES data after comparison - {}".format(val1[val1['_merge'] == 'right_only']))
                LOG.info("DB data after comparsion - {}".format(val1[val1['_merge'] == 'left_only']))
                # breakpoint()
                # if len(val1[val1['_merge'] == 'right_only']) != 0:
                #     flag = False

                if len(val1[val1['_merge'] == 'left_only']) != 0:
                    pd.DataFrame(['printing left only--------', "****************"]).to_csv(
                        "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                        index=False, mode='a')
                    (val1[val1['_merge'] == 'left_only']).to_csv(
                        "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                        index=False, mode='a')
                    flag = False
                if "test_individual_raw_stats_es_vs_db" not in tc_name:
                    if (es_response["_metadata"])["total_count"] != (db_response["_metadata"])["total_count"]:
                        LOG.info("ES vs DB count not matching")
                        LOG.info("DB count - {}".format((db_response["_metadata"])["total_count"]))
                        LOG.info("ES count - {}".format((es_response["_metadata"])["total_count"]))
                        flag = False

                # if len(db_df) == len(es_df) and es_columns.sort() == db_columns.sort():
                #     pd.DataFrame(["Compare results---", "****************"]).to_csv(
                #         "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                #         index=False, mode='a')
                #     db_df.sort_index(axis=1).reset_index(
                #         drop=True).compare(es_df.sort_index(axis=1).reset_index(drop=True)).to_csv(
                #         "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                #         index=False, mode='a')

        except Exception as ex:
            LOG.info("Not executed")
            LOG.info(ex)
            flag = False
            return flag, zero_flag

        return flag, zero_flag

    def trellis_data_validator(self, response, inside_records=None, invalid_values_check=None, raw_stats=None,
                               child_ous_check=None, ou_uuid=None, child_ou_uuids=None, columns=None,
                               associated_users_check=None, ou_users=None):
        executed_flag = True  # exception; no data
        valid_data_flag = True  # ou validation flag
        try:
            response_data = response["records"]
            if inside_records:
                response_data = (response["records"][0])["records"]
            response_df = pd.json_normalize(response_data)

            if type(response_df) != type(None):
                if raw_stats and invalid_values_check:
                    for eachCol in response_df.columns.to_list():
                        if eachCol.startswith("raw_stats."):
                            if len(response_df.loc[(response_df[eachCol] < 0)]) != 0:
                                valid_data_flag = False
                if raw_stats and child_ous_check:
                    unique_ou_uuids = (response_df.ou_id.unique()).tolist()
                    if ou_uuid not in unique_ou_uuids:
                        valid_data_flag = False
                    unique_ou_uuids.remove(ou_uuid)
                    if set(unique_ou_uuids) != set(child_ou_uuids):
                        valid_data_flag = False
                if not raw_stats and invalid_values_check:
                    response_df = pd.json_normalize(response_data, "section_responses", columns,
                                                    record_prefix="section_response.")
                    invalid_scores_df = response_df.loc[(response_df['score'] >= 100) | (response_df['score'] < 0) | (
                            response_df['section_response.score'] < 0) | (
                                                                response_df['section_response.score'] >= 100)]
                    if len(invalid_scores_df) != 0:
                        valid_data_flag = False
                if not raw_stats and child_ous_check:
                    unique_ou_uuids = (response_df.org_id.unique()).tolist()
                    if ou_uuid not in unique_ou_uuids:
                        valid_data_flag = False
                    unique_ou_uuids.remove(ou_uuid)
                    if set(unique_ou_uuids) != set(child_ou_uuids):
                        valid_data_flag = False
                if not raw_stats and associated_users_check:
                    response_df = pd.json_normalize(response_data, "section_responses", columns,
                                                    record_prefix="section_response.")
                    org_user_uuids = (response_df.org_user_id.unique()).tolist()
                    if set(org_user_uuids) != set(ou_users):
                        valid_data_flag = False
                if raw_stats and associated_users_check:
                    org_user_uuids = (response_df.org_user_id.unique()).tolist()
                    if set(org_user_uuids) != set(ou_users):
                        valid_data_flag = False

            else:
                executed_flag = False
        except Exception as ex:
            LOG.info("exception in trellis data validator")
            LOG.info(ex)
            executed_flag = False
            return executed_flag, valid_data_flag

        return executed_flag, valid_data_flag

    def aggs_db_data_validator(self, response_flow1, response_flow2, exclude_columns=None, sort_column_name=None,
                               tc_identifier=None, sort_values_inside_columns_str=None):
        flag = True
        data_flag = True
        try:
            if len(response_flow1) == 0:
                LOG.info("existing flow not having data")
                data_flag = True

            else:
                # breakpoint()
                if tc_identifier:
                    list1 = ["Merged results - " + tc_identifier]
                    df = pd.DataFrame(list1)
                    df.to_csv("log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True, index=False, mode='a')

                if len(response_flow2) == 0:
                    LOG.info("new flow not having data")
                    if len(response_flow1) != 0:
                        response_flow1.to_csv("log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                                              index=False, mode='a')
                        LOG.info('existing_flow is having data but new_flow is not having data')

                    data_flag = False

                if exclude_columns:
                    response_flow1.drop(columns=exclude_columns, inplace=True)
                    response_flow2.drop(columns=exclude_columns, inplace=True)

                if sort_values_inside_columns_str:
                    for col in response_flow1.columns:
                        if col in sort_values_inside_columns_str:
                            response_flow1[col] = response_flow1[col].map(lambda x: sorted(x, key=lambda y: str(y)))
                    for col in response_flow2.columns:
                        if col in sort_values_inside_columns_str:
                            response_flow2[col] = response_flow2[col].map(lambda x: sorted(x, key=lambda y: str(y)))

                if sort_column_name:
                    response_flow1 = response_flow1.sort_values(sort_column_name)
                    response_flow2 = response_flow2.sort_values(sort_column_name)
                # whether or not the count is matching print the difference in the csv

                response_flow1 = response_flow1.astype(str)
                response_flow2 = response_flow2.astype(str)
                response_flow1 = response_flow1.applymap(lambda s: s.strip() if type(s) == str else s)
                response_flow2 = response_flow2.applymap(lambda s: s.strip() if type(s) == str else s)
                cols = response_flow1.columns.values.tolist()
                LOG.info("Existing flow length---- {}".format(len(response_flow1)))
                LOG.info("new flow count - {}".format(len(response_flow2)))
                merge_res_val1 = pd.merge(response_flow1, response_flow2, on=cols, how='outer', indicator=True)
                if len(merge_res_val1[merge_res_val1['_merge'] == 'left_only']) == 0 and len(
                        merge_res_val1[merge_res_val1['_merge'] == 'right_only']) == 0:
                    LOG.info("************** There is no difference in DAtaFrames *************")
                    flag = True
                if len(merge_res_val1[merge_res_val1['_merge'] == 'left_only']) != 0:
                    LOG.info("Length of the records present in existing flow but not in new flow...{} ".format(
                        len(merge_res_val1[merge_res_val1['_merge'] == 'left_only'])))
                    (merge_res_val1[merge_res_val1['_merge'] == 'left_only']).to_csv(
                        "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True, index=False, mode='a')

                    if len(response_flow1) == len(response_flow2):

                        pd.DataFrame(["after compare values---", "**********"]).to_csv(
                            "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True, index=False,
                            mode='a')

                        if response_flow1.shape == response_flow2.shape:

                            val1 = response_flow1.reset_index(drop=True).compare(
                                response_flow2.reset_index(drop=True))

                            LOG.info(
                                "DB data after comparsion - {}".format(
                                    merge_res_val1[merge_res_val1['_merge'] == 'left_only']))
                            LOG.info(
                                "DB data after comparsion - {}".format(
                                    merge_res_val1[merge_res_val1['_merge'] == 'right_only']))

                            if len(val1) != 0:
                                LOG.info("existing_flow v/s new_flow data is not matching")
                                LOG.info("data mismatches - {}".format(val1))
                                val1.to_csv("log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                                            index=False,
                                            mode='a')
                                flag = False

                    elif len(response_flow2) > len(response_flow1):
                        LOG.info("New flow is having more records compared to existing flow ")
                        LOG.info("existing flow  count - {}".format(len(response_flow1)))
                        LOG.info("new flow count - {}".format(len(response_flow2)))

                    else:
                        LOG.info("Existing_flow and new_flow count not matching")
                        LOG.info("existing flow  count - {}".format(len(response_flow1)))
                        LOG.info("new flow count - {}".format(len(response_flow2)))
                        flag = False

        except Exception as ex:
            LOG.info("Exception {}".format(ex))
            LOG.info("Not executed")
            flag = False
            return flag, data_flag

        return flag, data_flag

    def comparing_es_vs_db_user_reports(self, es_response, db_response, columns=False, unique_identifier=None):
        flag = True
        zero_flag = True
        tc_name = str(inspect.stack()[1][3])
        # breakpoint()

        if unique_identifier is not None:
            df = pd.DataFrame([unique_identifier])
            df.to_csv(
                "log_updates/" + str(inspect.stack()[1][3])
                + '.csv', header=True,
                index=False, mode='a')
        try:

            if tc_name == "test_fixed_intervals_raw_stat_orgs_es_vs_db":

                es_df = pd.json_normalize(es_response['records'][0], max_level=1)
                db_df = pd.json_normalize(db_response['records'][0], max_level=1)



            else:
                es_response = json.dumps(es_response, sort_keys=True)
                es_df = pd.DataFrame([flatten_json(json.loads(es_response))])
                db_response = json.dumps(db_response, sort_keys=True)
                db_df = pd.DataFrame([flatten_json(json.loads(db_response))])

            es_df = es_df.round(decimals=2)
            es_df = es_df.astype(str)

            # db_df_col = list([flatten_json(db_response)][0].keys())

            db_df = db_df.round(decimals=2)
            db_df = db_df.astype(str)
            if len(es_df.columns.values) != len(db_df.columns.values):
                LOG.info("Length of column not matching ")
                flag = False
            if set(db_df.columns.values) != set(es_df.columns.values):
                LOG.info("Column values arent similar")
                flag = False

            if columns:
                columns = " records_0_".join(columns).split(" ")
                columns.pop(0)
                name_col = columns[0]
            if not columns:
                # columns = np.intersect1d(es_df_col, db_df_col)
                columns = db_df.columns.values.tolist()
            # breakpoint()
            val1 = pd.merge(db_df, es_df, on=list(columns), how='outer', indicator=True)
            # LOG.info("ES data after comparison - {}".format(val1[val1['_merge'] == 'right_only']))
            LOG.info("DB data after comparsion with ES Data-- {}".format(val1[val1['_merge'] == 'left_only']))
            if len(val1[val1['_merge'] == 'left_only']) != 0:
                if tc_name in ["test_trellis_dev_es", "test_individual_raw_stats"]:
                    db_df['full_name'].to_csv(
                        "log_updates/" + str(inspect.stack()[1][3])
                        + '.csv', header=True,
                        index=False, mode='a')
                elif tc_name in ["test_fixed_intervals_org_list_es_vs_db"]:
                    db_df['records_0_org_name'].to_csv(
                        "log_updates/" + str(inspect.stack()[1][3])
                        + '.csv', header=True,
                        index=False, mode='a')
                elif tc_name in ["test_fixed_intervals_raw_stat_orgs_es_vs_db"]:
                    db_df['name'].to_csv(
                        "log_updates/" + str(inspect.stack()[1][3])
                        + '.csv', header=True,
                        index=False, mode='a')
                else:
                    db_df['records_0_full_name'].to_csv(
                        "log_updates/" + str(inspect.stack()[1][3])
                        + '.csv', header=True,
                        index=False, mode='a')

                # if len(val1[val1['_merge'] == 'right_only']) != 0:
                #     flag = False
                if len(val1[val1['_merge'] == 'left_only']) != 0:
                    val1[val1['_merge'] == 'left_only'].reset_index(drop=True).to_csv(
                        "log_updates/" + str(inspect.stack()[1][3])
                        + '.csv', header=True,
                        index=False, mode='a')
                    flag = False
                pd.DataFrame(["comapring", "**********"]).to_csv(
                    "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True, index=False, mode='a')
                db_df.reset_index(
                    drop=True).compare(es_df.reset_index(drop=True)).to_csv(
                    "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                    index=False, mode='a')

                if tc_name not in ["test_trellis_dev_es", "test_fixed_intervals_raw_stat_orgs_es_vs_db"]:
                    if (es_response["_metadata"])["total_count"] != (db_response["_metadata"])["total_count"]:
                        LOG.info("ES vs DB count not matching")
                        LOG.info("DB count - {}".format((db_response["_metadata"])["total_count"]))
                        LOG.info("ES count - {}".format((es_response["_metadata"])["total_count"]))
                        flag = False
                if tc_name in ["test_fixed_intervals_raw_stat_orgs_es_vs_db"]:
                    if (es_response["count"]) != (db_response["count"]):
                        LOG.info("ES vs DB count not matching")
                        LOG.info("DB count - {}".format(db_response["count"]))
                        LOG.info("ES count - {}".format(es_response["count"]))
                        flag = False

        except Exception as ex:
            LOG.info("Not executed")
            LOG.info(ex)
            flag = False
            return flag, zero_flag

        return flag, zero_flag

    def comparing_es_vs_db_without_prefix_trellis_1(self, es_response, db_response, sort_column_name=None, columns=None,
                                                    sort_values_inside_columns=None,
                                                    sort_values_inside_columns_dict=None,
                                                    unique_identifier=None):
        flag = True
        zero_flag = True
        tc_name = str(inspect.stack()[1][3])
        LOG.info("tc_name--{}".format(tc_name))
        # import pdb;pdb.set_trace()
        # breakpoint()
        df = pd.DataFrame([unique_identifier])
        df.to_csv(
            "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
            index=False, mode='a')

        try:

            if len(((es_response['records'])[0])['records']) == 0:
                LOG.info("ES not having data")

                zero_flag = False

            else:
                # breakpoint()
                es_response = es_response['records'][0]['records']
                # es_response = json.dumps(es_response, sort_keys=True)

                es_df = pd.json_normalize(es_response)

                es_df = es_df.sort_values(['full_name', 'org_user_id'])
                es_df = es_df.round()
                es_df = es_df.astype(str)
                # es_df=es_df.sort_index(axis=1)

            if len(((db_response['records'])[0])['records']) == 0:
                LOG.info("DB not having data")
                zero_flag = False
            else:
                db_response = db_response['records'][0]['records']
                # db_response = json.dumps(db_response, sort_keys=True)
                db_df = pd.json_normalize(db_response)
                db_df = db_df.sort_values(['full_name', 'org_user_id'])

                db_df = db_df.round()
                db_df = db_df.astype(str)
                val1 = pd.merge(db_df, es_df, on=db_df.columns.to_list(),
                                how='outer', indicator=True)
                LOG.info("ES data after comparison - {}".format(val1[val1['_merge'] == 'right_only']))
                LOG.info("DB data after comparsion - {}".format(val1[val1['_merge'] == 'left_only']))

                if len(val1[val1['_merge'] == 'left_only']) != 0:
                    pd.DataFrame(['printing left only--------', "****************"]).to_csv(
                        "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                        index=False, mode='a')
                    (val1[val1['_merge'] == 'left_only']).to_csv(
                        "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                        index=False, mode='a')
                    flag = False
                    if "test_individual_raw_stats_es_vs_db" not in tc_name:
                        if (es_response["_metadata"])["total_count"] != (db_response["_metadata"])["total_count"]:
                            LOG.info("ES vs DB count not matching")
                            LOG.info("DB count - {}".format((db_response["_metadata"])["total_count"]))
                            LOG.info("ES count - {}".format((es_response["_metadata"])["total_count"]))
                            flag = False
                    # breakpoint()
                    if len(set(es_df.columns.values.tolist()) - set(db_df.columns.values.tolist())) != 0:
                        LOG.info("Columns are missing in ES ---{}".format(
                            set(es_df.columns.values.tolist()) - set(db_df.columns.values.tolist())))
                        flag = False
                    else:
                        pd.DataFrame(["Compare results---", "****************"]).to_csv(
                            "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                            index=False, mode='a')
                        db_df.reset_index(drop=True).compare(es_df.reset_index(drop=True)).to_csv(
                            "log_updates/" + str(inspect.stack()[1][3]) + '.csv', header=True,
                            index=False, mode='a')

        except Exception as ex:
            LOG.info("Not executed")
            LOG.info(ex)
            flag = False
            return flag, zero_flag

        return flag, zero_flag

    def trellis_data_df_flatenner(self, data_df):

        ## normalising the df data
        json_struct = json.loads(data_df.to_json(orient="records"))
        df_flat = pd.io.json.json_normalize(json_struct)

        ## normalising the df data - level1 - section responses
        columnsn = df_flat.columns.to_list()
        columnsn.remove("report.section_responses")
        json_structn = json.loads(df_flat.to_json(orient="records"))
        testdf = pd.json_normalize(json_structn, "report.section_responses", meta=columnsn,
                                   record_prefix="report.section_responses.")

        ## normalising the df data - level2 - feature responses
        columnsn1 = testdf.columns.to_list()
        columnsn1.remove("report.section_responses.feature_responses")
        json_structn1 = json.loads(testdf.to_json(orient="records"))
        resultdf = pd.json_normalize(json_structn1, "report.section_responses.feature_responses", meta=columnsn1,
                                     record_prefix="report.section_responses.feature_responses.")

        return resultdf
    
    def trellis_calculate_ado_ticket_portion(self, workitem_response, assignee_name):

        workitem_details = (workitem_response["records"][0])
        story_points = 0
        if "story_points" in workitem_details:
            story_points = workitem_details["story_points"]
        elif "story_point" in workitem_details:
            story_points = workitem_details["story_point"]
        status_list = workitem_details["status_list"]
        assignee_list = workitem_details["assignee_list"]
        assignee_list_df = pd.json_normalize(assignee_list)
        assignee_column_name = "assignee"
        end_date_column_name = "end_time"
        start_date_column_name = "start_time" 
        if "field_value" in assignee_list_df.columns.values.tolist():
            assignee_column_name = "field_value"
            end_date_column_name = "end_date"
            start_date_column_name = "start_date" 
        assignee_names = assignee_list_df[assignee_column_name].tolist()
        ## validating the single assignee workitem
        single_assignee_flag = False
        if (len(assignee_list) == 2) and ("UNASSIGNED" in assignee_names) :
            single_assignee_flag = True

        ticket_portion_calc = 0
        storypoints_portion_calc = 0
        if len(assignee_list) == 1 or single_assignee_flag:
            ticket_portion_calc = 1.0
            storypoints_portion_calc = story_points
        elif len(assignee_list) > 1:
            ## normalising the asignee list and appending assignee time, total time, ticket portion values
            assignee_list_df[end_date_column_name] = assignee_list_df[end_date_column_name]/1000
            assignee_list_df[end_date_column_name] = assignee_list_df[end_date_column_name].astype(int)
            assignee_list_df[start_date_column_name] = assignee_list_df[start_date_column_name]/1000
            assignee_list_df[start_date_column_name] = assignee_list_df[start_date_column_name].astype(int)
            assignee_list_df["assignee_time"] =  assignee_list_df[end_date_column_name] - assignee_list_df[start_date_column_name]
            ## removing the unassigned assignee details
            assignee_list_df = assignee_list_df[assignee_list_df[assignee_column_name] != 'UNASSIGNED']
            assignee_list_df["total_time"] = assignee_list_df["assignee_time"].sum()
            assignee_list_df["ticket_portion"] = assignee_list_df["assignee_time"]/assignee_list_df["total_time"]
            assignee_list_df["storypoints_portion"] = assignee_list_df["ticket_portion"]*story_points
            ## filtering the required assignee data and calculating the required ticket portion and story points portion 
            res_df = assignee_list_df[assignee_list_df[assignee_column_name] == assignee_name]
            ticket_portion_calc = res_df["ticket_portion"].sum()
            storypoints_portion_calc = res_df["storypoints_portion"].sum()

        return story_points, ticket_portion_calc, storypoints_portion_calc


    def trellis_calculate_ado_ticket_portion_v2(self, workitem_response, assignee_name, status_ids):
        workitem_details = (workitem_response["records"][0])
        workitem_key = workitem_details["key"]
        story_points = 0
        if "story_points" in workitem_details:
            story_points = workitem_details["story_points"]
        elif "story_point" in workitem_details:
            story_points = workitem_details["story_point"]
        assignee_list = workitem_details["assignee_list"]
        assignee_list_df = pd.json_normalize(assignee_list)
        assignee_column_name = "assignee"
        end_date_column_name = "end_time"
        start_date_column_name = "start_time" 
        if "field_value" in assignee_list_df.columns.values.tolist():
            assignee_column_name = "field_value"
            end_date_column_name = "end_date"
            start_date_column_name = "start_date" 
        assignee_names = assignee_list_df[assignee_column_name].tolist()
        status_ids_new = [str(num) for num in status_ids]
        status_list = workitem_details["status_list"]
        status_list_df = pd.json_normalize(status_list)
        status_list_df = status_list_df[status_list_df['status_id'].isin(status_ids_new)]
        status_list_df[end_date_column_name] = status_list_df[end_date_column_name]/1000
        status_list_df[end_date_column_name] = status_list_df[end_date_column_name].astype(int)
        status_list_df[start_date_column_name] = status_list_df[start_date_column_name]/1000
        status_list_df[start_date_column_name] = status_list_df[start_date_column_name].astype(int)
        req_intervals =  status_list_df[['start_time', 'end_time']].values.tolist()
        ## validating the single assignee workitem
        single_assignee_flag = False
        if (len(assignee_list) == 2) and ( ("UNASSIGNED" in assignee_names) or ('_UNASSIGNED_' in assignee_names) ) :
            single_assignee_flag = True

        ## normalising the asignee list and appending assignee time, total time, ticket portion values
        assignee_list_df[end_date_column_name] = assignee_list_df[end_date_column_name]/1000
        assignee_list_df[end_date_column_name] = assignee_list_df[end_date_column_name].astype(int)
        assignee_list_df[start_date_column_name] = assignee_list_df[start_date_column_name]/1000
        assignee_list_df[start_date_column_name] = assignee_list_df[start_date_column_name].astype(int)
        # assignee_list_df["assignee_time"] =  assignee_list_df[end_date_column_name] - assignee_list_df[start_date_column_name]
        ## removing the unassigned assignee details
        assignee_list_df = assignee_list_df[~(assignee_list_df[assignee_column_name].isin(['_UNASSIGNED_','UNASSIGNED']))]
        assignee_list_df['req_time'] = assignee_list_df.apply(lambda x: self.overlappingtime_calculator(x['start_time'], x['end_time'], req_intervals),axis=1)
        assignee_list_df["total_time"] = assignee_list_df["req_time"].sum()
        assignee_list_df["ticket_portion"] = assignee_list_df["req_time"]/assignee_list_df["total_time"]
        assignee_list_df["storypoints_portion"] = assignee_list_df["ticket_portion"]*story_points
        ## filtering the required assignee data and calculating the required ticket portion and story points portion 
        res_df = ""
        if type(assignee_name) == type([]):    
            res_df = assignee_list_df[assignee_list_df[assignee_column_name].isin(assignee_name)]
        else:
            res_df = assignee_list_df[assignee_list_df[assignee_column_name] == assignee_name]
        ticket_portion_calc = res_df["ticket_portion"].sum()
        storypoints_portion_calc = res_df["storypoints_portion"].sum()
        assignee_time = res_df["req_time"].sum()

        if len(assignee_list) == 1 or single_assignee_flag:
            ticket_portion_calc = 1.0
            storypoints_portion_calc = story_points

        return story_points, ticket_portion_calc, storypoints_portion_calc, assignee_time


    def overlappingtime_calculator(self, start_interval, end_interval, intervals_list):
        main_interval = [start_interval, end_interval]
        # Initialize the overlapping time counter
        overlapping_time = 0
        # Iterate through each interval in the list and check for overlap
        for interval in intervals_list:
            start = max(main_interval[0], interval[0])
            end = min(main_interval[1], interval[1])
            overlap = max(0, end - start)
            overlapping_time += overlap
        return overlapping_time

