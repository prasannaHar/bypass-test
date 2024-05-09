import datetime
import json
import logging
from calendar import calendar, monthrange
from src.utils.widget_reusable_functions import WidgetReusable
from dateutil.relativedelta import relativedelta
from src.utils.api_reusable_functions import ApiReusableFunctions
import random
from src.utils.cicd_payload_generator import generate_cicd_job_runs_values_payload

# from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class Mapping_library_helper:

    def __init__(self, generic_helper):
        # pass
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.env_info = self.generic.get_env_based_info()
        self.connection_info = self.generic.get_connect_info()
        self.widget_reusable = WidgetReusable(generic_helper)
        self.api_helper_obj = ApiReusableFunctions(generic_helper)

    def scm_release(self, release):
        release_dict = {
            "$begins":
                release

        }
        return release_dict

    def dora_change_failure_rate_filter(self, gt, lt, diff_filter_values=None):
        filter = {"time_range": {"$gt": gt, "$lt": lt}}
        if diff_filter_values is not None:
            filter.update(diff_filter_values)

        return filter

    def dora_deployment_freq_filter(self, arg_ou_ids, time_range, diff_filter_values=None, any_other_params={}):
        filter = {"time_range": time_range}
        if diff_filter_values is not None:
            filter.update(diff_filter_values)

        payload_dump = {
            "filter": filter,
            "ou_ids": arg_ou_ids,
            "stacks": [
                "pipelines"
            ]
        }

        if any_other_params:
            payload_dump.update(any_other_params)

        return payload_dump

    def release_dict(self, release, labels, commit_branch, source_branch, target_branch):
        dict = {
            "tags": self.scm_release(release=release),
            "labels": self.scm_release(release=labels),
            "commit_branch": self.scm_release(release=commit_branch),
            "source_branch": self.scm_release(release=source_branch),
            "target_branch": self.scm_release(release=target_branch)
        }
        return dict

    def commit_tag_commit_branch_dict(self, tags=None, commit_branch=None):
        dict = {}
        if tags:
            dict['tags'] = self.scm_release(release=tags)
        if commit_branch:
            dict['commit_branch'] = self.scm_release(release=commit_branch)

        return dict

    def scm_workflow_config(self):
        workflow_release = self.env_info("workflow_release")
        workflow_deployment = self.env_info("workflow_deployment")
        workflow_hotfix = self.env_info("workflow_hotfix")
        workflow_defect = self.env_info("workflow_defect")

        release = self.release_dict(release=[workflow_release['release']], labels=[workflow_release['labels']],
                                    commit_branch=[workflow_release['commit_branch']],
                                    source_branch=[workflow_release['source_branch']],
                                    target_branch=[workflow_release['target_branch']])
        deployment = self.release_dict(release=[workflow_deployment['release']], labels=[workflow_deployment['labels']],
                                       commit_branch=[workflow_deployment['commit_branch']],
                                       source_branch=[workflow_deployment['source_branch']],
                                       target_branch=[workflow_deployment['target_branch']])
        hotfix = self.release_dict(release=[workflow_hotfix['release']], labels=[workflow_hotfix['labels']],
                                   commit_branch=[workflow_hotfix['commit_branch']],
                                   source_branch=[workflow_hotfix['source_branch']],
                                   target_branch=[workflow_hotfix['target_branch']])
        defect = self.release_dict(release=[workflow_defect['release']], labels=[workflow_defect['labels']],
                                   commit_branch=[workflow_defect['commit_branch']],
                                   source_branch=[workflow_defect['source_branch']],
                                   target_branch=[workflow_defect['target_branch']])
        scm_config = {
            "release": release,
            "deployment": deployment,
            "hotfix": hotfix,
            "defect": defect
        }

        return scm_config

    def workflow_development_stage(self, name, type, values_list, lower_value, upper_value, mapping_id=None):
        stage = {
            "name": name,
            "order": 0,
            "event": {
                "any_label_added": False,
                "type": type,
                "values": values_list
            },
            "lower_limit_value": lower_value,
            "lower_limit_unit": "DAYS",
            "upper_limit_value": upper_value,
            "upper_limit_unit": "DAYS",
            "mapping_id": mapping_id
        }

        return stage

    def fixed_stages(self, name, order, type, lower_limit, upper_limit, params=None):
        fixed_stages = {
            "name": name,
            "order": order,
            "event": {
                "any_label_added": False,
                "type": type,
                "params": params
            },
            "lower_limit_value": lower_limit,
            "lower_limit_unit": "SECONDS",
            "upper_limit_value": upper_limit,
            "upper_limit_unit": "SECONDS"
        }

        return fixed_stages

    def deployment_freq_cicd(self, int_ids, calculation_field, values_list, application, filters={}):
        deployment_freq = {
            "integration_id": int_ids[0],
            "integration_ids": int_ids,
            "calculation_field": calculation_field,
            "application": application,
            "filters": {
                "deployment_frequency": {
                    "integration_type": "CICD",
                    "event": {
                        "any_label_added": False,
                        "type": "CICD_JOB_RUN",
                        "values": values_list,
                        "selectedJob": "ALL"
                    },
                    "calculation_field": calculation_field
                }
            }
        }

        if filters:
            deployment_freq["filters"]["deployment_frequency"].update({
                "filter": filters
            })

        if application == "harnessng":
            deployment_freq["filters"]["deployment_frequency"].update({
                "is_ci_job": filters.get("is_ci_job", True),
                "is_cd_job": filters.get("is_cd_job", True)
            })

        return deployment_freq

    def deployment_freq_IM(self, int_id, filter, calculation_field="issue_updated_at", application="jira"):
        """"filter":{
            "status_categories":[
            "Done"
            ],
            "exclude":{},
            "partial_match":{}
            }"""

        deployment_frequency = {
            "application": application,
            "calculation_field": calculation_field,
            "filters": {
                "deployment_frequency": {
                    "integration_type": "IM",
                    "filter": filter
                }
            },
            "integration_id": int_id,
            "integration_ids": [int(int_id)],
        }

        return deployment_frequency

    def deployment_freq_SCM(self, scm_filters, int_id, calculation_field="pr_merged_at"):
        deployment_frequency = {
            "integration_id": int_id,
            "filters": {
                "deployment_frequency": {
                    "integration_type": "SCM",
                    "scm_filters":
                        scm_filters

                }
            },
            "calculation_field": calculation_field
        }
        return deployment_frequency

    def deployment_freq_SCM_new(self, scm_filters, int_id, calculation_field="pr_merged_at", integration_ids=None,
                                deployment_criteria="pr_merged", deployment_route="pr"):
        deployment_frequency = {
            "integration_id": int_id,
            "integration_ids": integration_ids,
            "calculation_field": calculation_field,
            "filters": {
                "deployment_frequency": {
                    "calculation_field": calculation_field,
                    "deployment_criteria": deployment_criteria,
                    "deployment_route": deployment_route,
                    "integration_type": "SCM",
                    "integration_type": "SCM",
                    "scm_filters":
                        scm_filters
                }
            },
            "calculation_field": calculation_field
        }
        return deployment_frequency

    def mean_time_to_restore(self):
        """*****Hardcoding the values as of now as underdevelopment*****"""
        mean_time_to_restore = {
            "issue_management_integrations": [
                "jira"
            ],
            "fixed_stages": [
                {
                    "order": 0,
                    "name": "Lead time to First Commit",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_COMMIT_CREATED"
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 1,
                    "name": "PR Creation Time",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_CREATED"
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 2,
                    "name": "Time to First Comment",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_REVIEW_STARTED"
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 3,
                    "name": "Approval Time",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_APPROVED",
                        "params": {
                            "last_approval": [
                                "True"
                            ]
                        }
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 4,
                    "name": "Merge Time",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_MERGED",
                        "scm_filters": {
                            "target_branch": {
                                "$begins": [
                                    "bugfix",
                                    "fix",
                                    "bug"
                                ]
                            },
                            "commit_branch": {
                                "$begins": [
                                    "bugfix",
                                    "fix",
                                    "bug"
                                ]
                            },
                            "labels": {
                                "$begins": [
                                    "bugfix",
                                    "fix",
                                    "bug"
                                ]
                            }
                        }
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                }
            ],
            "fixed_stages_enabled": True,
            "event": "ticket_created",
            "starting_event_is_commit_created": False,
            "starting_event_is_generic_event": False,
            "pre_development_custom_stages": [],
            "post_development_custom_stages": []
        }

        return mean_time_to_restore

    def change_failure_rate_scm(self, int_id, failed_deployment_scm_filters, total_deployment_scm_filter):
        change_failure_rate = {
            "integration_id": int_id,
            "filters": {
                "integration_type": "SCM",
                "failed_deployment": {
                    "scm_filters": failed_deployment_scm_filters
                },
                "total_deployment": {
                    "scm_filters": total_deployment_scm_filter
                }
            }
        }

        return change_failure_rate

    def deployment_filter_scm(self, calculation_field, deployment_criteria, deployment_route, deployment_scm_filters):
        deployment = {
            "calculation_field": calculation_field,
            "deployment_criteria": deployment_criteria,
            "deployment_route": deployment_route,
            "integration_type": "SCM",
            "scm_filters": deployment_scm_filters
        }
        return deployment

    def change_failure_rate_scm_new(self, int_id, integration_ids, failed_deployment_filters, total_deployment_filter,
                                    is_absolute=False):
        change_failure_rate = {
            "integration_id": int_id,
            "integration_ids": integration_ids,
            "calculation_field": "pr_merged_at",
            "is_absolute": False,
            "filters": {
                "failed_deployment": failed_deployment_filters,
                "total_deployment": total_deployment_filter
            }
        }

        return change_failure_rate

    def change_failure_rate_cicd(self, int_ids, calculation_field, failed_deployment, total_deployment,
                                 is_absolute, application):
        change_failure_rate = {
            "integration_id": int_ids[0],
            "integration_ids": int_ids,
            "calculation_field": calculation_field,
            "filters": {
                "failed_deployment": {
                    "integration_type": "CICD",
                    "event": {
                        "any_label_added": False,
                        "type": "CICD_JOB_RUN",
                        "values": failed_deployment.get("values", []),
                        "selectedJob": "ALL"
                    },
                    "calculation_field": calculation_field
                },
                "total_deployment": {
                    "integration_type": "CICD",
                    "event": {
                        "any_label_added": False,
                        "type": "CICD_JOB_RUN",
                        "values": total_deployment.get("values", []),
                        "selectedJob": "ALL"
                    },
                    "calculation_field": calculation_field
                }
            },
            "is_absolute": is_absolute,
            "application": application,
            "calculation_field": calculation_field
        }

        if failed_deployment.get("filters"):
            change_failure_rate["filters"]["failed_deployment"].update({
                "filter": failed_deployment["filters"]
            })

        if total_deployment.get("filters"):
            change_failure_rate["filters"]["total_deployment"].update({
                "filter": total_deployment["filters"]
            })

        if application == "harnessng":
            change_failure_rate["filters"]["failed_deployment"].update({
                "is_ci_job": failed_deployment.get("is_ci_job", True),
                "is_cd_job": failed_deployment.get("is_cd_job", True)
            })
            change_failure_rate["filters"]["total_deployment"].update({
                "is_ci_job": total_deployment.get("is_ci_job", True),
                "is_cd_job": total_deployment.get("is_cd_job", True)
            })

        return change_failure_rate

    def change_failure_rate_IM(self, int_id, failed_deployment_filter, total_deployment_filter,
                               calculation_field="issue_updated_at", application="jira", is_absolute=False):
        change_failure_rate = {
            "application": application,
            "calculation_field": calculation_field,
            "filters": {
                "failed_deployment": {
                    "integration_type": "IM",
                    "filter":
                        failed_deployment_filter

                },
                "total_deployment": {
                    "integration_type": "IM",
                    "filter":
                        total_deployment_filter
                }
            },
            "integration_id": int_id,
            "integration_ids": [int(int_id)],
            "is_absolute": is_absolute,
        }
        return change_failure_rate

    def lead_time_for_changes(self):
        """*********hard coding**************till changes are completely available"""
        lead_time_for_changes = {
            "issue_management_integrations": [
                "jira"
            ],
            "fixed_stages": [
                {
                    "order": 0,
                    "name": "Lead time to First Commit",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_COMMIT_CREATED"
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 1,
                    "name": "PR Creation Time",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_CREATED"
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 2,
                    "name": "Time to First Comment",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_REVIEW_STARTED"
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 3,
                    "name": "Approval Time",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_APPROVED",
                        "params": {
                            "last_approval": [
                                "True"
                            ]
                        }
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 4,
                    "name": "Merge Time",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_MERGED",
                        "scm_filters": {
                            "target_branch": {
                                "$begins": [
                                    "release"
                                ]
                            },
                            "commit_branch": {
                                "$begins": [
                                    "release"
                                ]
                            },
                            "labels": {
                                "$begins": [
                                    "release"
                                ]
                            }
                        }
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                }
            ],
            "fixed_stages_enabled": True,
            "event": "ticket_created",
            "starting_event_is_commit_created": False,
            "starting_event_is_generic_event": False,
            "pre_development_custom_stages": [],
            "post_development_custom_stages": []
        }

        return lead_time_for_changes

    def week_date_check(self, lt, gt):
        weekstart_gt = datetime.date.fromtimestamp(int(gt)).isocalendar()[1]
        year_gt = datetime.date.fromtimestamp(int(gt)).year
        month_gt = datetime.date.fromtimestamp(int(gt)).month
        weekstart_lt = datetime.date.fromtimestamp(int(lt)).isocalendar()[1]
        year_lt = datetime.date.fromtimestamp(int(lt)).year
        month_lt = datetime.date.fromtimestamp(int(lt)).month
        date_gt = int((datetime.date(year_gt, 1, 1) + relativedelta(weeks=+weekstart_gt)).strftime('%s')) - (86400 * 7)
        # date_lt = int((datetime.date(year_lt, 1, 1) + relativedelta(weeks=+weekstart_lt)).strftime('%s')) + (86400)
        date_lt = int((datetime.date(year_lt, 1, 1) + relativedelta(weeks=+weekstart_lt)).strftime('%s'))
        month_date_gt = int(datetime.date(year_gt, month_gt, 1).strftime('%s')) - 86400
        month_date_lt = int(datetime.date(year_lt, month_lt, 1).strftime('%s')) + 86400
        _, num_days = monthrange(year_gt, month_gt)
        last_day = datetime.date(year_gt, month_gt, num_days).strftime('%s')
        last_day_month = int(last_day) + 86400 + 3600 * 5 + 1799
        first_day = datetime.date(year_gt, month_gt, 1).strftime('%s')
        first_day_month = int(first_day) + 3600 * 5 + 1800

        return date_gt, date_lt, month_date_gt, month_date_lt, int(last_day_month), int(first_day_month)

    def drill_down_verification_monthly(self, month, month_list, lt, ou_id, list_url, widget,
                                        diff_filter_values=None):
        drill_list = []
        for i in month:
            for k in month_list:
                if i == k:
                    lt_cal = self.widget_reusable.get_last_day_of_month(i)
                    gt = k
                    if lt < lt_cal:
                        lt = lt
                    else:
                        lt = lt_cal

                    # for i in range(0, len(month)):
                    #     # breakpoint()
                    #     if i == 0:
                    #         # break
                    #         gt = int(gt)
                    #         lt = last_day_month
                    #
                    #     elif i == (len(month)):
                    #         # break
                    #         gt = month[i]['key']
                    #         print("new_gt-----", gt)
                    #         print("new_lt-----", lt)
                    #     else:
                    #         gt = month[i]['key']
                    #         date_gt, date_lt, month_date_gt, month_date_lt, last_day_month, first_day_month = self.week_date_check(
                    #             gt=gt, lt=lt)
                    #         lt = last_day_month

                    if widget == "change_failure_rate":
                        filter_drilldown = self.dora_change_failure_rate_filter(gt=int(gt), lt=lt,
                                                                                diff_filter_values=diff_filter_values)

                    elif widget == "deployment_frequency_report":
                        filter_drilldown = self.dora_change_failure_rate_filter(gt=int(gt), lt=lt,
                                                                                diff_filter_values=diff_filter_values)

                    payload_drilldown = {
                        "filter": filter_drilldown,
                        "ou_ids": [ou_id],
                        "widget": widget
                    }
                    print("payload_drilldown-----", payload_drilldown, "\n")
                    drilldown_resp = self.generic.execute_api_call(list_url, "post", data=payload_drilldown)
                    print("drilldown_resp----", json.dumps(drilldown_resp))
                    print(drilldown_resp['_metadata']['total_count'])
                    if month[i]['count'] != drilldown_resp['_metadata']['total_count']:
                        drill_list.append(
                            {month[i]['count']: drilldown_resp['_metadata']['total_count'], "time": "month"})

            return drill_list

    def week_from_date(self, gt, lt):
        # breakpoint()
        weeknumber_gt = datetime.date.fromtimestamp(int(gt)).isocalendar()[1]
        year_gt = datetime.date.fromtimestamp(int(gt)).isocalendar()[0]
        year_lt = datetime.date.fromtimestamp(int(lt)).year
        weeknumber_lt = datetime.date.fromtimestamp(int(lt)).isocalendar()[1]
        weekstart_date_gt = int((datetime.date(year_gt, 1, 1) + relativedelta(weeks=+weeknumber_gt)).strftime('%s')) - (
                86400 * 5) - 66600

        weekstart_date_lt = int((datetime.date(year_lt, 1, 1) + relativedelta(weeks=+weeknumber_lt)).strftime('%s')) - (
                86400 * 5) - 66600

        enddate_gt = int(weekstart_date_gt) + 604799
        # enddate_gt = int(weekstart_date_gt) + 518400

        enddate_lt = int(weekstart_date_lt) + 604799
        # 19800

        return weekstart_date_gt, weekstart_date_lt, enddate_gt, enddate_lt

    def drill_down_verification_weekly(self, week, week_list, lt, gt, ou_id, list_url, widget,
                                       diff_filter_values=None):
        drill_list = []
        # breakpoint()

        for k in week_list:
            i = week_list.index(k)
            if i == 0:
                gt = gt
            else:
                gt = k
            lt_cal = self.widget_reusable.get_week_end_date(k)

            if lt < lt_cal:
                lt1 = lt
            else:
                lt1 = lt_cal + 86399

            if widget == "change_failure_rate":
                # breakpoint()
                filter_drilldown = self.dora_change_failure_rate_filter(gt=int(gt), lt=lt1,
                                                                        diff_filter_values=diff_filter_values)

            elif widget == "deployment_frequency_report":

                filter_drilldown = self.dora_change_failure_rate_filter(gt=int(gt), lt=lt1,
                                                                    diff_filter_values=diff_filter_values)
                LOG.info(f"filter_drilldown----{filter_drilldown}")

            payload_drilldown = {
                "filter": filter_drilldown,
                "ou_ids": [ou_id],
                "widget": widget
            }

            drilldown_resp = self.generic.execute_api_call(list_url, "post", data=payload_drilldown)
            # print("drilldown_resp----",json.dumps(drilldown_resp))
            LOG.info("drilldown_resp['_metadata']['total_count']-----{}---{}".format(
                drilldown_resp['_metadata']['total_count'], week[i]['count']))
            # total_week_tickets=total_week_tickets+week[i]['count']

            if week[i]['count'] != drilldown_resp['_metadata']['total_count']:
                LOG.info("payload_drilldown-----{}".format(json.dumps(payload_drilldown)))

                drill_list.append({week[i]['count']: drilldown_resp['_metadata']['total_count'], "time": "week"})

        return drill_list

    def IM_deploy_change_failrate_workflow(self, name, filter, integration_id, failed_deployment_filter,
                                           total_deployment_filter, url, filter_update,
                                           failed_deployment_filter_update, total_deployment_filter_update):
        flagged_list = []
        # breakpoint()
        """Post call create a workflow - Validate teh same created workflow using get call
                   and finally delete the profile created using del option """
        # breakpoint()
        payload = {
            "name": name,
            "default_config": False,
            "description": "this is all in one json for workflow object.",
            "created_at": round(datetime.datetime.now().timestamp()),
            "updated_at": round(datetime.datetime.now().timestamp()),
            "lead_time_for_changes": self.lead_time_for_changes(),
            "mean_time_to_restore": self.mean_time_to_restore(),
            "deployment_frequency": self.deployment_freq_IM(filter=filter, int_id=int(integration_id)),
            "change_failure_rate": self.change_failure_rate_IM(int_id=int(integration_id),
                                                               failed_deployment_filter=failed_deployment_filter,
                                                               total_deployment_filter=total_deployment_filter),
            "is_new": True
        }
        LOG.info("Payload ----{}".format(json.dumps(payload)))
        resp = self.generic.execute_api_call(url, "post", data=payload)

        id = resp['id']
        # Use the get call to pull the details of the created id and validate with the i/p
        get_resp = self.generic.execute_api_call(url=url + "/" + id, request_type="get")
        if get_resp['name'] != name:
            flagged_list.append("Name is not matching in get and post request")
        if get_resp['deployment_frequency']['filters'] != payload['deployment_frequency']['filters']:
            flagged_list.append("Deployment request is not matching in get and post response")
        if get_resp['change_failure_rate']['filters'] != payload['change_failure_rate']['filters']:
            flagged_list.append("Change failure rate not matching get n post response")

        del payload['change_failure_rate']
        payload['deployment_frequency'] = self.deployment_freq_IM(
            filter=filter_update,
            int_id=int(integration_id))
        payload['change_failure_rate'] = self.change_failure_rate_IM(
            failed_deployment_filter=failed_deployment_filter_update,
            total_deployment_filter=total_deployment_filter_update,
            int_id=int(integration_id))

        payload.update({"id": id})
        payload.update(payload['deployment_frequency'])

        payload.update(payload['change_failure_rate'])
        LOG.info("Payload ----{}".format(json.dumps(payload)))

        updated_resp = self.generic.execute_api_call(url=url + "/" + id, request_type="put", data=payload)
        get_resp = self.generic.execute_api_call(url=url + "/" + id, request_type="get")
        # breakpoint()

        if get_resp['deployment_frequency']['filters'] != payload['deployment_frequency']['filters']:
            flagged_list.append("Deployment Freq in get response not matching to updated payload")

        if get_resp['change_failure_rate']['filters'] != payload['change_failure_rate']['filters']:
            flagged_list.append("change failure rate is not matching in get and updated post response")

        """Delete teh created profile"""
        resp_del = self.generic.execute_api_call(url=url + "/" + id, request_type='delete')
        get_resp = self.generic.execute_api_call(url=url + "/" + id, request_type="get")
        json_resp = json.loads(get_resp.text)
        if json_resp['status'] != 404:
            flagged_list.append("json_resp['status'] is not 404")
        if json_resp['message'] != "Could not find Velocity Config with id=" + id:
            flagged_list.append("json_resp['message'] is not as expected")

        assert len(flagged_list) == 0, "Test case failed with flagged_list failured---{}".format(flagged_list)

    def cfr_dora_scm_tc(self, create_api_reusable_funct_object, scm_int, calculation_field, deployment_criteria,
                        deployment_route, is_absolute=False):
        """Post call create a workflow - Validate teh same created workflow using get call
                      and finally delete the profile created using delete call ."""
        flagged_list = []
        LOG.info("**********************")
        name = "auto_" + create_api_reusable_funct_object.random_alpha_numeric_string(5)
        url = self.connection_info['base_url'] + self.api_info["velocity_config_api"]
        workflow_release = self.env_info['workflow_release']
        workflow_hotfix = self.env_info['workflow_hotfix']

        if deployment_route == "commit":
            if deployment_criteria == "commit_merged_to_branch" and calculation_field == "commit_pushed_at":
                scm_filter = self.commit_tag_commit_branch_dict(commit_branch=workflow_release['commit_branch'])
                failed_deployment_scm_filters = self.commit_tag_commit_branch_dict(
                    commit_branch=workflow_hotfix['commit_branch'])

            elif deployment_criteria == "commit_with_tag" and calculation_field == "committed_at":
                scm_filter = self.commit_tag_commit_branch_dict(tags=workflow_release['tags'])
                failed_deployment_scm_filters = self.commit_tag_commit_branch_dict(tags=workflow_hotfix['tags'])

            elif deployment_criteria == "commit_merged_to_branch_with_tag" and (
                    calculation_field == "commit_pushed_at" or calculation_field == "committed_at"):
                scm_filter = self.commit_tag_commit_branch_dict(tags=workflow_release['tags'],
                                                                commit_branch=workflow_release['commit_branch'])
                failed_deployment_scm_filters = self.commit_tag_commit_branch_dict(tags=workflow_hotfix['tags'],
                                                                                   commit_branch=workflow_hotfix[
                                                                                       'commit_branch'])

        else:
            scm_filter = self.release_dict(release=workflow_release['release'], labels=workflow_release['labels'],
                                           commit_branch=workflow_release['commit_branch'],
                                           source_branch=workflow_release['source_branch'],
                                           target_branch=workflow_release['target_branch'])

            failed_deployment_scm_filters = self.release_dict(release=workflow_hotfix['release'],
                                                              labels=workflow_hotfix['labels'],
                                                              commit_branch=workflow_hotfix['commit_branch'],
                                                              source_branch=workflow_hotfix['source_branch'],
                                                              target_branch=workflow_hotfix['target_branch'])

        total_deployment = self.deployment_filter_scm(calculation_field=calculation_field,
                                                      deployment_criteria=deployment_criteria,
                                                      deployment_route=deployment_route,
                                                      deployment_scm_filters=scm_filter)

        # breakpoint()

        failed_deployment_filter = self.deployment_filter_scm(calculation_field=calculation_field,
                                                              deployment_criteria=deployment_criteria,
                                                              deployment_route=deployment_route,
                                                              deployment_scm_filters=failed_deployment_scm_filters)

        LOG.info("SCM filter-----{}".format(scm_filter))
        LOG.info("failed_deployment_scm_filters---{}".format(failed_deployment_scm_filters))
        if is_absolute:
            total_deployment = {}
            change_failure_rate = self.change_failure_rate_scm_new(
                integration_ids=[int(scm_int)],
                failed_deployment_filters=failed_deployment_filter,
                total_deployment_filter=total_deployment,
                int_id=int(scm_int), is_absolute=True)
        else:
            change_failure_rate = self.change_failure_rate_scm_new(
                integration_ids=[int(scm_int)],
                failed_deployment_filters=failed_deployment_filter,
                total_deployment_filter=total_deployment,
                int_id=int(scm_int), is_absolute=False)
        payload = {
            "name": name,
            "default_config": False,
            "description": "this is all in one json for workflow object.",
            "created_at": round(datetime.datetime.now().timestamp()),
            "updated_at": round(datetime.datetime.now().timestamp()),
            "lead_time_for_changes": self.lead_time_for_changes(),
            "mean_time_to_restore": self.mean_time_to_restore(),
            "deployment_frequency": self.deployment_freq_SCM_new(scm_filters=scm_filter,
                                                                 int_id=int(scm_int),
                                                                 integration_ids=[
                                                                     int(scm_int)],
                                                                 calculation_field=calculation_field,
                                                                 deployment_criteria=deployment_criteria,
                                                                 deployment_route=deployment_route),
            "change_failure_rate": change_failure_rate,
            "is_new": True
        }

        payload_change_failurerate = payload["change_failure_rate"]
        # breakpoint()
        del payload_change_failurerate['filters']['failed_deployment']['integration_type']
        LOG.info("Payload ----{}".format(json.dumps(payload)))
        resp = self.generic.execute_api_call(url, "post", data=payload)
        #
        id = resp['id']

        """Use the get call to pull the details of the created id and validate with the i/p"""

        get_resp = self.generic.execute_api_call(url=url + "/" + id, request_type="get")
        if get_resp['name'] != name:
            flagged_list.append("Name is not matching in get and post request")
        if get_resp['deployment_frequency'] != payload['deployment_frequency']:
            flagged_list.append("Deployment request is not matching in get and post response")
        if get_resp['change_failure_rate'] != payload['change_failure_rate']:
            flagged_list.append("Change failure rate not matching get n post response")
        # breakpoint()

        """update the payload by deleting certain fields"""
        if deployment_route != "commit":

            del payload['change_failure_rate']['filters']['failed_deployment']['scm_filters']['tags']
            del payload['deployment_frequency']['filters']['deployment_frequency']['scm_filters']['labels']
            payload.update({"id": id})
            LOG.info("Payload ----{}".format(json.dumps(payload)))
            updated_resp = self.generic.execute_api_call(url=url + "/" + id, request_type="put", data=payload)

            """get the response and validate if the update took place"""

            get_resp = self.generic.execute_api_call(url=url + "/" + id, request_type="get")
            #
            if get_resp['deployment_frequency'] != payload['deployment_frequency']:
                flagged_list.append("Deployment Freq in get response not matching to updated payload")

            if get_resp['change_failure_rate'] != payload['change_failure_rate']:
                flagged_list.append("change failure rate is not matching in get and updated post response")

        """Delete teh created profile"""
        resp_del = self.generic.execute_api_call(url=url + "/" + id, request_type='delete')
        get_resp = self.generic.execute_api_call(url=url + "/" + id, request_type="get")
        json_resp = json.loads(get_resp.text)
        if json_resp['status'] != 404:
            flagged_list.append("json_resp['status'] is not 404")
        if json_resp['message'] != "Could not find Velocity Config with id=" + id:
            flagged_list.append("json_resp['message'] is not as expected")

        assert len(flagged_list) == 0, "Test case failed with flagged_list failured---{}".format(flagged_list)

    def create_cicd_workflow_profile(self, df_filters=dict(), cfr_filters=dict(),
                                     mttr_filters=dict(), ltfc_filters=dict(), profile_filters=dict()):
        """
        Function to create the workdlow profile with the given filters.

        Args:
        df_filters (dict): Deployment Frequency filter details.
        cfr_filters (dict): Change Failure Rate configuration details.
        mttr_filters (dict): Mean Time to Restore configuration details.
        ltfc_filters (dict): Lead Time for Changes configuration details.
        profile_filters (dict): Filters to be applied on the workflow profile.

        Returns:
        profile_details (dict): Workflow Profile details (Name and ID).
        """

        workflow_profile = dict()

        # TODO: Add values support in the workflow profile.
        # values_list = self.generic.env["workflow_values_list_cicd"]
        values_list = []

        name = "auto_" + self.api_helper_obj.random_alpha_numeric_string(5)

        # cicd_int = self.workflow_int_ids['CICD']
        cicd_int_ids = profile_filters.get("integration_ids")
        cicd_product_application = profile_filters.get("application", "harnessng")
        ou_ids = [profile_filters.get("ou_id")]
        workflow_url = self.generic.connection["base_url"] + \
                       self.generic.api_data["velocity_config_api"]

        payload = {
            "name": name,
            "default_config": False,
            "associated_ou_ref_ids": ou_ids,
            "description": "Workflow profile created through pytest-data-validation framework.",
            "created_at": round(datetime.datetime.now().timestamp()),
            "updated_at": round(datetime.datetime.now().timestamp()),
            "lead_time_for_changes": self.lead_time_for_changes(),
            "mean_time_to_restore": self.mean_time_to_restore(),
            "deployment_frequency": self.deployment_freq_cicd(
                int_ids=cicd_int_ids,
                calculation_field=df_filters.get("calculation_field", "start_time"),
                values_list=df_filters.get("values_list", values_list),
                application=cicd_product_application,
                filters=df_filters.get("filters", {})
            ),
            "change_failure_rate": self.change_failure_rate_cicd(
                int_ids=cicd_int_ids,
                calculation_field=cfr_filters.get("calculation_field", "start_time"),
                failed_deployment={
                    "values": cfr_filters.get("values_list", []),
                    "filters": cfr_filters.get("failed_deployment_filters", {})
                },
                total_deployment={
                    "values": cfr_filters.get("values_list", []),
                    "filters": cfr_filters.get("total_deployment_filters", {})
                },
                is_absolute=False,
                application=cicd_product_application
            ),
            "is_new": True
        }

        resp = self.generic.execute_api_call(workflow_url + "?there_is_no_cache=true", "post", data=payload)
        # LOG.info("resp----{}".format(json.dumps(resp)))
        id = resp['id']

        workflow_profile = {
            "name": name,
            "id": id
        }

        return workflow_profile

    def delete_workflow_profile(self, workflow_id):
        """
        Function to delete the workflow profile.
        Args:
        workflow_id (str): Workflow ID to delete.
        """
        workflow_url = self.generic.connection["base_url"] + \
                       self.generic.api_data["velocity_config_api"] + f"/{workflow_id}"

        resp = self.generic.execute_api_call(workflow_url + "?there_is_no_cache=true", "delete")

        if resp != "":
            LOG.info(f"Delete workflow profile response: {resp}")
        LOG.info(f"Workflow Profile '{workflow_id}' deleted.")

    def calculate_record_cnt_by_group(self, application, day_wise_records=[], week_wise_records=[],
                                     month_wise_records=[]):
        """
        This function helps count the records returned in day_wise_records, week_wise_records
        and month_wise_records.

        Args:
        application (str): Application for which CICD DF widget records are returned.
        day_wise_records (list): List of day-wise records in CICD DF widget.
        week_wise_records (list): List of week-wise records in CICD DF widget.
        month_wise_records (list): List of month-wise records in CICD DF widget.

        Returns: Tuple containing day-wise, week-wise and month-wise job execution count.
        """
        day_count = 0
        week_count = 0
        month_count = 0

        for data in day_wise_records:
            stack_count = 0
            for pipeline in data.get("stacks", []):
                stack_count = stack_count + pipeline["count"]
            day_count = day_count + stack_count

        for data in week_wise_records:
            stack_count = 0
            for pipeline in data.get("stacks", []):
                stack_count = stack_count + pipeline["count"]
            week_count = week_count + stack_count

        for data in month_wise_records:
            stack_count = 0
            for pipeline in data.get("stacks", []):
                stack_count = stack_count + pipeline["count"]
            month_count = month_count + stack_count

        return day_count, week_count, month_count

    def select_key_for_drilldown(self, application, records=[], already_parsed_records=[]):
        """
        Function to select a random record from the list for widget -> drilldown consistency verification.
        Args:
        application (str): Application name for which widget is verified.
        records (list): List of records to select the random records from.
        already_parsed_records (list): List of keys already verified.

        Returns: A tuple of random record selected and its position in the 'records' list.
        """
        LOG.info(f"Selecting random key for drilldown for application = {application}...")
        stack_with_values = None
        record_selected = {}
        stack_index = 0

        # Will attempt 5 retries to discover a record with non-null values.
        max_try = 5

        if records:
            while not record_selected and max_try > 0:
                LOG.info(f"Random selection trial: {max_try}")
                stack_index = random.choice(range(len(records)))
                stack_with_values = records[stack_index]
                if stack_with_values.get("stacks") or stack_index in already_parsed_records:
                    record_selected = stack_with_values
                    break
                else:
                    max_try = max_try - 1

        LOG.info(f"Random record selected for drill-down-> Record Index: {stack_index},"
                 f" Record: {record_selected}")
        return record_selected, stack_index

    def dora_verify_filters_on_records(self, records, filters, values_list):
        """
        Function to verify the filters applied on the workflow profile. Dora DF widgets will
        inherently consider these filters while iterating over the records.
        Args:
        records (list): List of records to select the random records from.
        filters (dict): Filters applied in the workflow profile.

        Returns: List of records not matching the filter condition.
        """

        record_field_names = self.generic.api_data["cicd_job_count_config"]
        unmatched_records = {}
        for key in filters.keys():
            unmatched_values_list = []

            if key == "exclude":
                for field, exclude_values in filters[key].items():
                    record_field_name = field
                    for field_mapping in record_field_names.values():
                        if field_mapping.get("filters") == field:
                            record_field_name = field_mapping.get("response")
                            break

                    if not isinstance(exclude_values, list):
                        unmatched_values_list.extend(
                            [record for record in records if record.get(record_field_name) == exclude_values]
                        )
                    else:
                        unmatched_values_list.extend(
                            [record for record in records if record.get(record_field_name) in exclude_values]
                        )

            elif key == "partial_match":
                for field, filter_sub_string in filters[key].items():

                    record_field_name = field
                    for field_mapping in record_field_names.values():
                        if field_mapping.get("x_axis") == field:
                            record_field_name = field_mapping.get("response")
                            break

                    if filter_sub_string.get("$contains"):
                        unmatched_values_list.extend(
                            [
                                record for record in records if
                                filter_sub_string["$contains"] not in record.get(record_field_name, "")
                            ]
                        )
                    elif filter_sub_string.get("$begins"):
                        unmatched_values_list.extend(
                            [
                                record for record in records if
                                not record.get(record_field_name, "").startswith(filter_sub_string["$begins"])
                            ]
                        )
            else:
                record_field_name = key
                for field_mapping in record_field_names.values():
                    if field_mapping.get("filters") == key:
                        record_field_name = field_mapping.get("response")
                        break

                if not isinstance(filters[key], list):
                    unmatched_values_list.extend(
                        [record for record in records if record.get(record_field_name) != filters[key]]
                    )
                else:
                    unmatched_values_list.extend(
                        [record for record in records if record.get(record_field_name) not in filters[key]]
                    )

            if unmatched_values_list:
                unmatched_records[key] = unmatched_values_list

        if values_list:
            unmatched_values_list.extend(
                [record for record in records if record.get("cicd_job_id") not in values_list]
            )

            if unmatched_values_list:
                unmatched_records["values"] = unmatched_values_list

        return unmatched_records

    def cicd_drilldown_widget(self, org_ids, time_range, page=None, page_size=None, widget="deployment_frequency_report"):
        """
        Function to fetch records for CICD Drilldown widget.

        Args:
        org_ids (list): Organization units on which to execute the drilldown widget.
        time_range (dict): Time range for drilldown API execution.
        page (int): Page number to fetch the drilldown response.
        page_size (int): Maximum records in a drilldown API call.

        Returns: Drilldown widget response.
        """
        # CICD DF drilldown URL.
        dora_widget_drilldown_url = self.generic.connection["base_url"] + \
                                       self.generic.api_data["dora_drilldown_list_api"]

        drilldown_payload = self.dora_deployment_freq_filter(
            arg_ou_ids=org_ids,
            time_range=time_range,
            any_other_params={"widget": widget}
        )
        if page != None and page_size != None:
            drilldown_payload.update({
                "page": page,
                "page_size": page_size
            })
        LOG.info(f"CICD drilldown payload: {json.dumps(drilldown_payload)}")

        # CICD DF drilldown API call execution.
        drilldown_resp = self.generic.execute_api_call(
            dora_widget_drilldown_url + "?there_is_no_cache=true", "post", data=drilldown_payload
        )
        return drilldown_resp

    def calculate_time_range_for_drilldown_widget(self, record, record_index, group_type, widget_time_range):
        """
        Function to calculate the time range for the DF drilldown widget.

        Args:
        record (dict): Stack selected from DF widget for drilldown.
        record_index (int): Position of record in the list.
        group_type (str): Group based on which the records are combined.
        widget_time_range (dict): Time range selected for widget from which drilldown is selected..

        Returns: Time range object for executing the DF drilldown API.
        """
        if group_type == "day":
            time_range = self.widget_reusable.get_day_start_end_dates(
                epoch_time=record["key"]
            )
        else:
            time_range = {}

            # For the first record, the start time needs to be the time selected from DF widget.
            if record_index == 0:
                time_range["$gt"] = int(widget_time_range["$gt"])
            else:
                time_range["$gt"] = record["key"]

            if group_type == "week":
                lt_cal = self.widget_reusable.get_week_end_date(record["key"], get_day_end_time=True)
            else:
                lt_cal = self.widget_reusable.get_last_day_of_month(record["key"])

            # For the last record in 'week' or 'month' group, the ending time needs to be
            # selected as per the DF widget.
            if int(widget_time_range["$lt"]) <= lt_cal:
                time_range["$lt"] = int(widget_time_range["$lt"])
            else:
                time_range["$lt"] = lt_cal

        return time_range

    def dora_widget_drill_down_verification(self, application, records, org_ids, widget_time_range={},
                                            group_type="day", filters={}, values_list = [],
                                            skip_record_match_check=False,
                                            widget="deployment_frequency_report"):
        """
        Function to compare the record count obtained from DF widget and DF drill-down.
        Args:
        application (str): Application name for which widget is verified.
        records (list): List of records to select the random records from.
        org_ids (list): OU IDs on which the widget API calls has to be executed.
        widget_time_range (dict): Time range selected for Widget API call.
        group_type (str): Type through which the records are grouped ('day', 'week', 'month').
        skip_record_match_check (Boolean): Skip the record match check. If True, only count of records will be verified.
        filters (dict): Filters applied in the workflow profile.

        Returns: Tuple containing list of unmatched records and the random record selected for drilldown.
        """

        verify_count = 0
        parsed_elements = []
        unmatched_values = []

        # Widget -> Drilldown verification will be done thrice by selecting random values.
        while verify_count != 3:
            verify_count = verify_count + 1
            LOG.info(f"Verification round: {verify_count} ...")

            # Selecting a random stack from the list.
            random_record, record_index = self.select_key_for_drilldown(
                application=application,
                records=records,
                already_parsed_records=parsed_elements
            )
            if not random_record.get("key"):
                LOG.info("Skipping this verification round no random record "
                         "found with non-null values.")
                continue

            # Calculate time range for Drilldown widget.
            time_range = self.calculate_time_range_for_drilldown_widget(
                record=random_record, record_index=record_index,
                group_type=group_type, widget_time_range=widget_time_range
            )

            # Execute Drilldown widget API call.
            drilldown_resp = self.cicd_drilldown_widget(
                org_ids=org_ids, time_range=time_range, widget=widget)

            drilldown_record_count = 0
            if drilldown_resp:
                # Fetching count of records from CICD DF drilldown widget.
                drilldown_record_count = drilldown_resp['_metadata']['total_count']

            # Calculating count of records in the stack selected from CICD DF widget.
            df_widget_record_count = 0
            for stack in random_record.get("stacks", []):
                df_widget_record_count = df_widget_record_count + stack["count"]

            LOG.info("[Data Consistency Check] Record count from DF widget - df_widget_record_count: "
                     f"{df_widget_record_count}")
            LOG.info("[Data Consistency Check] Record count from DF widget drilldown - drilldown_record_count: "
                     f"{drilldown_record_count}")

            if df_widget_record_count != drilldown_record_count:
                unmatched_values.append(
                    {
                        "record": random_record,
                        "df_widget_count": df_widget_record_count,
                        "df_drilldown_record_count": drilldown_record_count
                    }
                )

            if filters and not skip_record_match_check:
                LOG.info(f"[Data Consistency Check] Verifying the filters applied on the widgets: {filters}")
                filter_mismatch = self.dora_verify_filters_on_records(
                        records=drilldown_resp["records"],
                        filters=filters,
                        values_list = values_list
                    )
                if filter_mismatch:
                    unmatched_values.append(filter_mismatch)

                LOG.info(f"[Data Consistency Check] Filters applied on the widgets verification complete.")

            if unmatched_values:
                break

            parsed_elements.append(record_index)

        LOG.info(f"Mis-matched records list: {unmatched_values}")

        return unmatched_values, random_record

    def execute_dora_profile_widget_api(self, widget_url, org_id, time_range):
        """
        Execute Dora widget API call with the given parameters.
        Args:
        widget_url (str): Widget URL to call.
        org_id (list): List of Org IDs to consider while calling the API.
        time_range (dict): Start time and end time for widget API call.

        Returns: Widget API response (dict).
        """
        # CFR Post API call payload.
        payload = self.dora_deployment_freq_filter(
            arg_ou_ids=org_id,
            time_range=time_range
        )
        LOG.info("CICD CFR widget payload----{}".format(json.dumps(payload)))

        try:
            resp = self.generic.execute_api_call(
                widget_url + "?there_is_no_cache=true", "post", data=payload
            )
        except Exception as ex:
            LOG.warning("Error occurred while executing Dora widget API."
                        f"exception---{ex}")
            raise Exception(ex)

        return resp

    def cfr_band_calculation(self, failure_rate):
        """
        Calculate CFR band based on the value of the failure rate.
        Args:
        failure_rate (float): Change failure rate value in percentage.

        Returns: Band value according to the failure rate provided.
        """
        if failure_rate <= 15:
            band = "ELITE"
        elif 15 < failure_rate <= 30:
            band = "HIGH"
        elif 30 < failure_rate <= 45:
            band = "MEDIUM"
        else:
            band = "LOW"

        return band

    def dora_fetch_selected_pipelines(self, integration_ids,
                                      pipeline_field = "job_normalized_full_name"):
        """
        Function to select random pipelines for Dora workflow profile.
        Args:
        integration_ids (list): List of integrations from which pipelines to select.

        Returns: List of pipelines selected randomly.
        """

        random_selected_records = {}
        # Generate the payload to be used for executing the `/job_runs` API.
        job_run_value_payload = generate_cicd_job_runs_values_payload(
            arg_integration_ids=integration_ids,
            arg_fields=[pipeline_field],
            arg_req_dynamic_filters=[("integration_ids", integration_ids)],
        )

        # URL preparation for `/job_runs` API call.
        url = (
            self.generic.connection["base_url"] + \
                self.generic.api_data["CICD_JOB_COUNT_WIDGET_JOB_RUN_VALUES"]
        )

        # Executing the API call to fetch the filter values.
        filters_value_data = self.generic.execute_api_call(
            url=url, request_type="POST", data=job_run_value_payload
        )
        records = filters_value_data["records"][0][pipeline_field]
        records_count = len(records)
        if records_count > 1:
            random_selection = random.sample(
                records, random.choice(range(1, records_count))
            )

            random_selected_records["job_ids"] = [record["cicd_job_id"] for record in random_selection]
            random_selected_records["job_names"] = [record["key"] for record in random_selection]

        elif records_count == 1:
            random_selected_records["job_ids"] = [records[0]["cicd_job_id"]]
            random_selected_records["job_names"] = [records[0]["key"]]

        return random_selected_records