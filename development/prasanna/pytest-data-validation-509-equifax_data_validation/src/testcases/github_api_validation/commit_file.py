"this is a sample python file"
import logging

import pandas as pd
import random
import string

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
def generate_random_string(length):
    alphanumeric_characters = string.ascii_letters + string.digits
    return ''.join(random.choice(alphanumeric_characters) for _ in range(length))


def edit_csv_file():
    # Generate a random string of length 10
    random_string = generate_random_string(10)
    # print(random_string)
    data = {"string_sample": [random_string]}

    df = pd.DataFrame(data)
    LOG.info(f"df----{df}")
    df.to_csv("test.csv", header=True, index=False)


