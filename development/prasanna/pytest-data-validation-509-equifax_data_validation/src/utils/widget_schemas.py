from voluptuous import Schema, Required, All, Any, Length, Object, Optional, ALLOW_EXTRA, Extra


class Schemas:

    def create_widget_issue_backlog_trend(self):
        create_widget_issue_backlog_trend = Schema(
            {

                Required("records"): Schema([{
                    Required("key"): All(str),
                    Required("additional_key"): All(str),
                    Required("median"): All(int),
                    Required("min"): All(int),
                    Required("max"): All(int),
                    Required("mean"): All(float),
                    Required("p90"): All(int),
                    Required("total_tickets"): All(int),
                    Required("total_story_points"): All(int),

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),

                })
            }
        )
        return create_widget_issue_backlog_trend

    def create_widget_scm_files_report(self):
        create_widget_scm_files_report = Schema(
            {

                Required("records"): Schema([{
                    Required("key"): All(str),
                    Required("repo_id"): All(str),
                    Required("project"): All(str),
                    Required("count"): All(int),

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Optional("next_page"): All(int),
                    Required("has_next"): All(bool),
                    Required("total_count"): All(int),

                })
            }
        )
        return create_widget_scm_files_report

    def create_widget_sonarqube_code_complexity(self):
        create_widget_sonarqube_code_complexity = Schema(
            {

                Required("records"): Schema([{
                    Required("key"): All(str),
                    Required("additional_key"): All(str),
                    Optional("median"): All(int),
                    Optional("min"): All(int),
                    Optional("max"): All(int),
                    Optional("sum"): All(int),
                    Optional("total"): All(int),
                    Optional("duplicated_density"): All(float),

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),
                    Required("total_count"): All(int),

                })
            }
        )
        return create_widget_sonarqube_code_complexity

    def create_widget_bullseye_code_coverage(self):
        create_widget_bullseye_code_coverage = Schema(
            {

                Required("records"): Schema([{
                    Required("key"): All(str),
                    Required("additional_counts"): Schema({
                        Required("bullseye_coverage_metrics"): Schema({
                            Required("functions_covered"): All(int),
                            Required("functions_uncovered"): All(int),
                            Required("total_functions"): All(int),
                            Required("function_percentage_coverage"): All(float),
                            Required("decisions_coverage"): All(int),
                            Required("decisions_uncovered"): All(int),
                            Required("total_decisions"): All(int),
                            Required("decision_percentage_coverage"): All(float),
                            Required("conditions_covered"): All(int),
                            Required("conditions_uncovered"): All(int),
                            Required("total_conditions"): All(int),
                            Required("condition_percentage_coverage"): All(float)

                        })
                    })
                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),
                    Required("total_count"): All(int),

                })
            }
        )
        return create_widget_bullseye_code_coverage

    def create_widget_scm_commit_single_stat(self):
        create_widget_scm_commit_single_stat = Schema(
            {

                Required("records"): Schema([{
                    Required("key"): All(str),
                    Required("additional_key"): All(str),
                    Required("count"): All(int),
                    Required("total_lines_added"): All(int),
                    Required("total_lines_removed"): All(int),
                    Required("total_lines_changed"): All(int),
                    Required("total_files_changed"): All(int),
                    Required("avg_change_size"): All(float),
                    Required("median_change_size"): All(int),
                    Required("pct_new_lines"): All(float),
                    Required("pct_refactored_lines"): All(float),
                    Required("pct_legacy_refactored_lines"): All(float),

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Optional("next_page"): All(int),
                    Required("has_next"): All(bool),
                    Required("total_count"): All(int),

                })
            }
        )
        return create_widget_scm_commit_single_stat

    def create_lead_time_for_change(self):
        create_lead_time_for_change = Schema(
            {

                Required("records"): Schema([{
                    Required("count"): All(int),
                    Required("band"): All(str),
                    Required("lead_time"): All(int),

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),

                })
            }
        )
        return create_lead_time_for_change

    def create_time_to_restore_service(self):
        create_time_to_restore_service = Schema(
            {

                Required("records"): Schema([{
                    Required("count"): All(int),
                    Required("band"): All(str),
                    Required("recover_time"): All(int),

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),

                })
            }
        )
        return create_time_to_restore_service

    def create_deployment_frequency(self, application):
        create_deployment_frequency = Schema({
                Required("time_series"): Schema({
                    Required("day"): Schema([{
                        Required("key"): All(int),
                        Required("count"): All(int),
                        Required("additional_key"): All(str),
                        Optional("stacks"): Schema([{
                            Required("count"): All(int),
                            Required("key"): All(str)
                        }])
                    }]),
                    Required("week"): Schema([{
                        Required("key"): All(int),
                        Required("count"): All(int),
                        Required("additional_key"): All(str),
                        Optional("stacks"): Schema([{
                            Required("count"): All(int),
                            Required("key"): All(str)
                        }])
                    }]),
                    Required("month"): Schema([{
                        Required("key"): All(int),
                        Required("count"): All(int),
                        Required("additional_key"): All(str),
                        Optional("stacks"): Schema([{
                            Required("count"): All(int),
                            Required("key"): All(str)
                        }])
                    }])
                }),
                Required("stats"): Schema({
                    Required("count_per_day"): All(float),
                    Optional("count_per_week"): All(float),
                    Optional("count_per_month"): All(float),
                    Required("band"): All(str),
                    Required("total_deployment"): All(int)
                })
            }
        )
        return create_deployment_frequency

    def cicd_deployment_frequency_drilldown(self):
        cicd_dora_df_drilldown = Schema({
            Required("records"): Schema([{
                    Required("status"): All(str),
                    Required("job_run_number"): All(int),
                    Required("cicd_user_id"): All(str),
                    Required("job_name"): All(str),
                    Required("job_normalized_full_name"): All(str),
                    Optional("project_name"): All(str),
                    Required("cicd_instance_name"): All(str),
                    Optional("repo_url"): All(str),
                }],
                extra=ALLOW_EXTRA
            ),
            Required("count"): All(int),
            Required("_metadata"): Schema({
                Required("page_size"): All(int),
                Required("page"): All(int),
                Optional("next_page"): All(int),
                Required("has_next"): All(bool),
                Required("total_count"): All(int)
            })
        })
        return cicd_dora_df_drilldown

    def create_failure_rate_frequency(self, application):
        create_cfr_widget = Schema({
                Required("time_series"): Schema({
                    Required("day"): Schema([{
                        Required("key"): All(int),
                        Required("count"): All(int),
                        Required("additional_key"): All(str),
                        Optional("stacks"): Schema([{
                            Required("count"): All(int),
                            Required("key"): All(str)
                        }])
                    }]),
                    Required("week"): Schema([{
                        Required("key"): All(int),
                        Required("count"): All(int),
                        Required("additional_key"): All(str),
                        Optional("stacks"): Schema([{
                            Required("count"): All(int),
                            Required("key"): All(str)
                        }])
                    }]),
                    Required("month"): Schema([{
                        Required("key"): All(int),
                        Required("count"): All(int),
                        Required("additional_key"): All(str),
                        Optional("stacks"): Schema([{
                            Required("count"): All(int),
                            Required("key"): All(str)
                        }])
                    }])
                }),
                Required("stats"): Schema({
                    Required("failure_rate"): All(float),
                    Required("band"): All(str),
                    Required("total_deployment"): All(int)
                },
                extra=ALLOW_EXTRA
                )
            }
        )

        return create_cfr_widget

    def create_issue_bounce_report(self):
        create_issue_bounce_report = Schema(
            {

                Required("records"): Schema([{
                    Optional("key"): All(str),
                    Required("additional_key"): All(str),
                    Required("median"): All(int),
                    Required("min"): All(int),
                    Required("max"): All(int),
                    Required("total_tickets"): All(int),

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),

                })
            }
        )
        return create_issue_bounce_report

    def create_issue_resolution_time_single_stat(self):
        create_issue_resolution_time_single_stat = Schema(
            {

                Required("records"): Schema([{
                    Optional("key"): All(str),
                    Optional("additional_key"): All(str),
                    Optional("median"): All(int),
                    Optional("min"): All(int),
                    Optional("max"): All(int),
                    Optional("mean"): All(float),
                    Optional("p90"): All(int),
                    Optional("total_tickets"): All(int),

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),

                })
            }
        )
        return create_issue_resolution_time_single_stat

    def create_issue_lead_time_trend_report(self):
        create_issue_resolution_time_single_stat = Schema(
            {

                Required("records"): Schema([{
                    Required("key"): All(str),
                    Required("data"): Schema([{
                        Required("key"): All(str),
                        Required("additional_key"): All(str),
                        Required("median"): All(int),
                        Required("mean"): All(float),
                        Required("count"): All(int),
                        Required("p90"): All(int),
                        Required("p95"): All(int),
                        Required("velocity_stage_result"):Schema({
                            Required("lower_limit_value"): All(int),
                            Required("lower_limit_unit"): All(str),
                            Required("upper_limit_value"): All(int),
                            Required("upper_limit_unit"): All(str),
                            Required("rating"): All(str),
                        })
                    }])

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),

                })
            }
        )
        return create_issue_resolution_time_single_stat

    def create_issue_resolution_time_by_stage(self):
        create_issue_resolution_time_by_stage = Schema(
            {

                Required("records"): Schema([{
                    Required("key"): All(str),
                    Required("additional_key"): All(str),
                    Required("median"): All(int),
                    Required("count"): All(int),
                    Required("mean"): All(float),
                    Required("p90"): All(int),
                    Required("p95"): All(int),
                    Required("velocity_stage_result"): Schema({
                        Required("lower_limit_value"): All(int),
                        Required("lower_limit_unit"): All(str),
                        Required("upper_limit_value"): All(int),
                        Required("upper_limit_unit"): All(str),
                        Required("rating"): All(str),
                    })

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),

                })
            }
        )
        return create_issue_resolution_time_by_stage

    def create_issues_report(self):
        create_issues_report = Schema(
            {

                Required("records"): Schema([{
                    Optional("key"): All(str),
                    Optional("additional_key"): All(str),
                    Optional("total_tickets"): All(int),
                    Optional("total_story_points"): All(int),
                    Optional("mean_story_points"): All(float),
                    Optional("total_unestimated_tickets"): All(int),

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),

                })
            }
        )
        return create_issues_report

    def create_widget_issue_time_across_stages(self):
        create_widget_issue_time_across_stages = Schema(
            {

                Required("records"): Schema([{
                    Optional("key"): All(str),
                    Optional("additional_key"): All(str),
                    Required("median"): All(int),
                    Required("min"): All(int),
                    Required("max"): All(int),
                    Required("mean"): All(float),
                    Required("p90"): All(int),
                    Required("p95"): All(int),
                    Required("total_tickets"): All(int),
                    Required("stage"): All(str),

                }],
                    extra=ALLOW_EXTRA
                ),
                Required("count"): All(int),
                Required("_metadata"): Schema({
                    Required("page_size"): All(int),
                    Required("page"): All(int),
                    Required("has_next"): All(bool),

                })
            }
        )
        return create_widget_issue_time_across_stages

    def cicd_job_count_report(self):
        cicd_job_count_report = Schema({
            Required("records"): Schema([{
                Required("key"): All(str),
                Required("count"): All(int),
            }],
                extra=ALLOW_EXTRA
            ),
            Extra: object,
            Required("count"): All(int),
            Required("_metadata"): Schema({
                Required("page_size"): All(int),
                Required("page"): All(int),
                Required("has_next"): All(bool),
            })
        })
        return cicd_job_count_report

    def cicd_job_count_report_drilldown_list(self):
        cicd_job_count_report_drilldown_list = Schema({
            Required("records"): Schema([{
                Required("id"): All(str),
                Required("cicd_job_id"): All(str),
                Required("job_run_number"): All(int)
            }],
                extra=ALLOW_EXTRA
            ),
            Required("count"): All(int),
            Required("_metadata"): Schema({
                Required("page_size"): All(int),
                Required("page"): All(int),
                Required("has_next"): All(bool),
                Required("total_count"): All(int)
            })
        })
        return cicd_job_count_report_drilldown_list

    def cicd_job_count_report_without_body(self):
        cicd_job_count_report_without_body = Schema({
            Required("status"): All(int),
            Required("error"): All(str),
            Required("message"): All(str),
            Extra: object
        })
        return cicd_job_count_report_without_body
