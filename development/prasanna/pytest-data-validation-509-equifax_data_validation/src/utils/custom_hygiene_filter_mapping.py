
custom_hygienes_vs_filters_mapping = {

    "No PM Acceptance":[
        ["issue_types",["STORY","IMPROVEMENT"]]
        ],
    "Missing UX Design":[
        ["status_categories",["Done"]]
        ],
    "No Business Priority":[
        ["priorities",["HIGHEST","HIGH","MEDIUM"]]
        ],
    "No Customer Use cases":[
        ["statuses",["DONE","IN PROGRESS","IN REVIEW","TO DO"]]
        ],
    "Missing Story Points":[
        ["missing_fields",{"story_points":True}],
        ["issue_types",["STORY","BUG"]]
        ],
    "Large Story Points > 5":[
        ["issue_types",["STORY","BUG"]],
        ["story_points",{"$gt":"5"}]
        ]

}


