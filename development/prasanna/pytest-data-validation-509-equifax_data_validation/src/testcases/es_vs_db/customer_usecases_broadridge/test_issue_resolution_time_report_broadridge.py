import logging
import pytest
from copy import deepcopy
from src.lib.core_reusable_functions import epoch_timeStampsGenerationForRequiredTimePeriods as TPhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TestIssuesReport:

    @pytest.mark.run(order=1)
    def test_issue_resolution_time_widget_vs_drilldown_broadridge_specific(self, create_generic_object,
                                    create_issue_resolution_time_report_object):
        """Validate issue resolution time report data consistency check -- validate median & total_tickets"""

        ou_ids_bulk = create_generic_object.env["customer_specific_ous"]
        across=create_generic_object.env["story_points_custom_field"]

        widgets_no_data_list = []
        drilldown_no_data_list = []
        widget_vs_drilldown_ticketcount_mismatch = []
        widget_vs_drilldown_median_mismatch = []
        not_executed_list = []
        for each_ou_ids in ou_ids_bulk:
            ou_ids = [ each_ou_ids ]
            no_of_months, gt, lt = TPhelper("LAST_MONTH")
            issue_resolve = {"$gt": gt, "$lt": lt}
            filters = {"exclude": {"stages": ["ANALYZING","OPEN","SPRINT READY","READY FOR QA","TO DO"]}}
            try:
                create_issue_resolution_time = create_issue_resolution_time_report_object.issue_resolution_time_report(
                    issue_resolve=issue_resolve, var_filters=filters, sort_xaxis="label_low-high",
                    across=across,sort=[{"id":"custom_field","desc":False}], arg_ou_id=ou_ids,
                    ou_user_filter_designation = True )
                assert create_issue_resolution_time, "widget is not created"
                if not create_issue_resolution_time:
                    widgets_no_data_list.append(each_ou_ids)
                    continue
                LOG.info("==== Issue resolution report data consistency with drilldown ====")
                ## widget key(story points) & total_tickets pair
                key_value = create_issue_resolution_time_report_object.issue_resolution_time_report(
                                    issue_resolve=issue_resolve, var_filters=filters, sort_xaxis="label_low-high",
                                    across=across,sort=[{"id":"custom_field","desc":False}], arg_ou_id=ou_ids, 
                                    ou_user_filter_designation = True, keys=True, additional_key=False )
                ## widget key(story points) & median pair
                key_value_median = create_issue_resolution_time_report_object.issue_resolution_time_report(
                                    issue_resolve=issue_resolve, var_filters=filters, sort_xaxis="label_low-high",
                                    across=across,sort=[{"id":"custom_field","desc":False}], arg_ou_id=ou_ids, 
                                    ou_user_filter_designation = True, keys=True, additional_key=False, required_item="median" )
                for key, count in key_value.items():
                    filters_drilldown = deepcopy(filters)
                    filters_drilldown["custom_fields"] = {"customfield_10002":[key]}
                    filters_drilldown["include_solve_time"]=True
                    drilldown = create_issue_resolution_time_report_object.issue_resolution_time_report_drilldown(
                        issue_resolve=issue_resolve, var_filters=filters_drilldown, across=across, arg_ou_id=ou_ids,
                        ou_user_filter_designation = True, key_option=across)
                    if drilldown:
                        ## checking for total_ticket consistency widget v/s drill-down
                        total_record_count = drilldown["_metadata"]["total_count"]
                        if not count == total_record_count:
                            ## [ou_id, key_story_points, widget_tickets_count, drilldown_tickets_count]
                            widget_vs_drilldown_ticketcount_mismatch.append([each_ou_ids, key, count, total_record_count])  
                        ## checking for median consistenct between widget v/s drilldown
                        widget_vs_drilldown_median_mismatch
                        solve_times = []
                        for record in range(total_record_count):
                            solve_times.append( ((drilldown['records'])[record])['solve_time'] )
                        solve_times.sort(reverse=True)
                        median_index = int(len(solve_times) / 2)
                        drill_down_median = solve_times[median_index]
                        if not (int(key_value_median[key]) == int(drill_down_median)):
                            ## [ou_id, key_story_points, widget_median, drilldown_median]
                            widget_vs_drilldown_median_mismatch.append([each_ou_ids, key, key_value_median[key],drill_down_median ])
                    else:
                        drilldown_no_data_list.append(each_ou_ids)
            except Exception as ex:
                not_executed_list.append(each_ou_ids)
                LOG.info(ex)

        LOG.info("Widgets with no data list {}".format(set(widgets_no_data_list)))
        LOG.info("drilldowns with no data list {}".format(set(drilldown_no_data_list)))
        LOG.info("ticket count mismatch list {}".format(widget_vs_drilldown_ticketcount_mismatch))
        LOG.info("median values mismatch list {}".format(set(widget_vs_drilldown_median_mismatch)))
        LOG.info("Not executed ous list {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "Not executed ous list {}".format(set(not_executed_list))
        assert len(widgets_no_data_list) == 0, "Widgets with no data list {}".format(
            set(widgets_no_data_list))
        assert len(drilldown_no_data_list) == 0, "drilldowns with no data list {}".format(
            set(drilldown_no_data_list))
        assert len(widget_vs_drilldown_ticketcount_mismatch) == 0, "ticket count mismatch list {}".format(
            widget_vs_drilldown_ticketcount_mismatch)
        assert len(widget_vs_drilldown_median_mismatch) == 0, "median values mismatch list {}".format(
            set(widget_vs_drilldown_median_mismatch))

