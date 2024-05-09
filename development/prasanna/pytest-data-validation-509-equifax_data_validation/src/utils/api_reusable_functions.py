import logging
from collections import defaultdict

import random
import string

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class ApiReusableFunctions:
    def __init__(self, generic_helper):
        self.generic = generic_helper

    def is_ascending(self, lst):
        for i in range(1, len(lst)):
            if lst[i] < lst[i - 1]:
                return False
        return True

    def is_descending(self, lst):
        for i in range(1, len(lst)):
            if lst[i] > lst[i - 1]:
                return False
        return True

    def random_alpha_numeric_string(self, len):
        # get random string of letters and digits
        source = string.ascii_lowercase + string.digits
        result_str = ''.join((random.choice(source) for i in range(len)))
        return result_str

    def check_matching_word(self, sentence, words):
        for i in words:
            j = i.split()
            for k in j:
                if sentence[0].lower().find(k.lower()) == 0:
                    return True

    def swap_dict_keys_vals(self, input_dict):

        swapped_dict = dict([(v, [k for k, v1 in input_dict.items() if v1 == v])
                    for v in set(input_dict.values())])

        return swapped_dict

    def fetch_record_count_from_datastore(self, datastore, filters, time_range=None,
                                          calculation_field="end_time", values_list = [],
                                          skip_fields=[]):
        """
        Function to filter the datastore based on filters provided and return count of
        matching records.

        Args:
        datastore (list): List of records fetched from the product.
        filters (dict): List of filters applied on the widget.
        time_range (dict): Time range to filter the records.
        calculation_field (str): Field to consider for record filters.
        values_list (list): List of specific pipelines selected.
        skip_fields (list): Fields to skip from datastore check.

        Returns: Count of matching records from the datastore.
        """

        record_field_names = self.generic.api_data["cicd_job_count_config"]
        matched_records_count = 0

        filter_keys = list(filters.keys())

        for record in datastore:
            if time_range:
                if not int(time_range.get("$gt")) <= int(record.get(calculation_field)/1000) <= \
                        int(time_range.get("$lt")):
                    continue

            if values_list and record["job_normalized_full_name"] not in values_list:
                    continue

            exclude_filter_match = True
            partial_filter_match = True
            exact_filter_match = True

            for key in filter_keys:
                field_to_skip = False

                if not exclude_filter_match or not partial_filter_match or not exact_filter_match:
                    break

                if key == "exclude":
                    for field, values in filters.get("exclude", {}).items():
                        if not exclude_filter_match:
                            break

                        if not isinstance(values, list):
                            values = [values]
                        values = set(values)

                        datastore_value = []
                        for datastore_field, mapping in record_field_names.items():
                            if mapping.get("filters") == field:
                                # Fields which need to be skipped from datastore check.
                                if datastore_field in skip_fields:
                                    field_to_skip = True
                                    break

                                record_field_name = datastore_field
                                datastore_value = record.get(record_field_name)
                                break

                        if field_to_skip:
                            continue

                        if not isinstance(datastore_value, list):
                            datastore_value = [datastore_value]
                        datastore_value = set(datastore_value)

                        if datastore_value and datastore_value.issubset(values):
                            exclude_filter_match = False
                            partial_filter_match = False
                            exact_filter_match = False

                elif key == "partial_match" and partial_filter_match:
                    for field, condition in filters.get("partial_match", {}).items():
                        if not partial_filter_match:
                            break

                        record_field_name = field
                        for datastore_field, mapping in record_field_names.items():
                            if mapping.get("x_axis") == field:
                                # Fields which need to be skipped from datastore check.
                                if datastore_field in skip_fields:
                                    field_to_skip = True
                                    break

                                record_field_name = datastore_field
                                break

                        if field_to_skip:
                            continue

                        if condition.get("$contains"):
                            substring_to_check = condition["$contains"]
                            value = record.get(record_field_name, "")
                            if isinstance(value, list):
                                for val in value:
                                    if not val.__contains__(substring_to_check):
                                        partial_filter_match = False
                                        exact_filter_match = False
                            else:
                                if not value.__contains__(substring_to_check):
                                    partial_filter_match = False
                                    exact_filter_match = False

                        elif condition.get("$begins"):
                            substring_to_check = condition["$begins"]
                            value = record.get(record_field_name, "")
                            if isinstance(value, list):
                                string_match = False
                                for val in value:
                                    if val.startswith(substring_to_check):
                                        string_match = True
                                        break

                                if not string_match:
                                    partial_filter_match = False
                                    exact_filter_match = False
                            else:
                                if not value.startswith(substring_to_check):
                                    partial_filter_match = False
                                    exact_filter_match = False

                elif exact_filter_match and key != "end_time":
                    record_field_name = key
                    for datastore_field, mapping in record_field_names.items():
                        if mapping.get("filters") == key:
                            # Fields which need to be skipped from datastore check.
                            if datastore_field in skip_fields:
                                field_to_skip = True
                                break

                            record_field_name = datastore_field
                            break

                    if field_to_skip:
                        continue

                    filter_value = filters[key]

                    if not isinstance(filter_value, list):
                        filter_value = [filter_value]
                    filter_value = set(filter_value)

                    datastore_value = record.get(record_field_name)
                    if not isinstance(datastore_value, list):
                        datastore_value = [datastore_value]
                    datastore_value = set(datastore_value)

                    if not datastore_value or not datastore_value.issubset(filter_value):
                        exact_filter_match = False

            if exclude_filter_match and partial_filter_match and exact_filter_match:
                matched_records_count = matched_records_count + 1

        return matched_records_count

    def cicd_fetch_time_range(self):
        # Getting epoc GMT time.
        gt, lt = self.generic.get_epoc_utc(
            self.generic.env["cicd_job_count_data_collection_span"], "days", lt_time_delta=2
        )
        time_range = {"$gt": gt, "$lt": lt}

        return time_range

    def list_vals_comparision(self, reference_vals, received_vals):
        """this logic helps to check the filters data validation 
        for example, if we have applied assignee filter in the widget level
        this logic will helps to 
        if all the drill-down tickets are related to selected assignees or not

        Args:
            reference_vals (list): base reference list of elements 
            received_vals (list): list of elements or list of list of elements
        """
        comparison = True
        try:
            received_vals_temp = []
            ## parsing the received_vals if received_vals of list of lists
            if type(received_vals[0]) == type([]):
                for sublist in received_vals:
                    cnt = 0
                    for item in sublist:
                        if item in reference_vals:
                            received_vals_temp.append(item)
                            cnt = 1
                            break
                    if cnt == 0:
                        comparison = False
                received_vals = received_vals_temp
            ## reference_vals v/s received_vals comparison
            if not all(item in reference_vals for item in received_vals):
                comparison = False
        except Exception as ex:
            LOG.info("exeception occured in filters validation {}".format(ex))
            return False
        return comparison
    
    def partial_match_filter_value_generator(self, strings, min_length=4):
        """ this function will return most commonly used sub-string """
        # Create a defaultdict to store substring counts
        substring_counts = defaultdict(int)
        # Iterate through each string
        for string in strings:
            # Generate all possible substrings
            for i in range(len(string)):
                for j in range(i + min_length, len(string) + 1):
                    substring = string[i:j]
                    if len(substring) >= min_length:
                        substring_counts[substring] += 1
        # Find the substring with the highest count
        max_count = max(substring_counts.values())
        most_common_substring = None
        for substring, count in substring_counts.items():
            if count == max_count:
                most_common_substring = substring
                break  # Exit the loop after finding the first substring with max count
        return most_common_substring
