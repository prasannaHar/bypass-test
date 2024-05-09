def tribe_level_lead_time_payload(gt, lt):
    return {
        "filter": {"issue_types": ["STORY"], "released_in": {"$gt": str(gt), "$lt": str(lt)},
                   "issue_resolved_at": {"$gt": "1677628800", "$lt": "1704067199"}
            , "status_categories": ["Done"], "velocity_config_id": "1be4931b-22b3-4b6f-8203-112d5174456a",
                   "integration_ids": ["8"],
                   "excludeVelocityStages": ["Ignore states", "DONE", "Other"], "calculateSingleState": True,
                   "work_items_type": "jira"},
        "ou_ids": ["338"],
        "ou_user_filter_designation": {"sprint": ["customfield_10300"]},
        "widget_id": "75036890-170f-11ee-9868-57f898a97936"}


def velocity_list_call_payload_for_tribes(gt, lt):
    return {"page": 0, "page_size": 500,
            "sort": [{"id": "Total_Time_In_stage", "desc": True}],
            "filter": {"issue_types": ["STORY"],
                       "released_in": {"$gt": str(gt), "$lt": str(lt)},
                       "issue_resolved_at": {"$gt": "1677628800", "$lt": "1704067199"},
                       "status_categories": ["Done"],
                       "velocity_config_id": "1be4931b-22b3-4b6f-8203-112d5174456a",
                       "integration_ids": ["8"], "velocity_stages": ["$$ALL_STAGES$$"],
                       "work_items_type": "jira",
                       "excludeVelocityStages": ["Ignore states", "DONE", "Other"]},
            "across": "none", "ou_ids": ["339"],
            "ou_user_filter_designation": {"sprint": ["customfield_10300"]},
            "ou_exclusions": ["none"], "widget_id": "75036890-170f-11ee-9868-57f898a97936"}


def release_table_payload_for_tribes(gt, lt):
    return {
        "filter": {"issue_types": ["STORY"], "released_in": {"$gt": str(gt), "$lt": str(lt)},
                   "issue_resolved_at": {"$gt": "1677628800", "$lt": "1704067199"}, "status_categories": ["Done"],
                   "velocity_config_id": "1be4931b-22b3-4b6f-8203-112d5174456a", "integration_ids": ["8"],
                   "excludeVelocityStages": ["DONE", "Ignore states", "Other"], "work_items_type": "jira"}, "page": 0,
        "page_size": 500, "across": "fix_versions", "ou_ids": ["334"],
        "ou_user_filter_designation": {"sprint": ["customfield_10300"]},
        "widget_id": "d7ec5730-5640-11ee-b2f2-27299e07410f"}


def list_payload_release_table(gt, lt):
    return {"page": 0, "page_size": 500, "filter": {"issue_types": ["STORY"],
                                                    "released_in": {"$gt": str(gt),
                                                                    "$lt": str(lt)},
                                                    "issue_resolved_at": {"$gt": "1677628800",
                                                                          "$lt": "1704067199"},
                                                    "status_categories": ["Done"],
                                                    "velocity_config_id": "1be4931b-22b3-4b6f-8203-112d5174456a",
                                                    "across": "fix_versions",
                                                    "integration_ids": ["8"],
                                                    "fix_versions": [
                                                        "Missing FN/LN on DE&A Legacy Billing Events"],
                                                    "work_items_type": "jira",
                                                    "excludeVelocityStages": ["DONE", "Ignore states",
                                                                              "Other"]},
            "ou_ids": ["334"], "ou_user_filter_designation": {"sprint": ["customfield_10300"]},
            "widget_id": "d7ec5730-5640-11ee-b2f2-27299e07410f"}


def lead_time_stories_payload(gt, lt):
    return {"page": 0, "page_size": 500, "sort": [{"id": "Total_Time_In_stage", "desc": True}],
            "filter": {"issue_types": ["STORY"],
                       "issue_resolved_at": {"$gt": str(gt), "$lt": str(lt)},
                       "velocity_config_id": "2d14654d-4f83-4b86-9fd9-b4da770e2044",
                       "integration_ids": ["8"], "velocity_stages": ["$$ALL_STAGES$$"],
                       "work_items_type": "jira", "excludeVelocityStages": ["TO DO"]},
            "across": "none", "ou_ids": ["325"],
            "ou_user_filter_designation": {"sprint": ["customfield_10300"]}, "ou_exclusions": ["none"],
            "widget_id": "66055e70-6f53-11ed-9ddb-51177dc6cfa1"}
