# * Imports
import os
import random
import time
import unittest

import pandas as pd
from faker import Faker

from src.main.database.controller import Controller
from src.main.utils import utils
from test import testutils


# ! add in controller every time the initialization of the BPlusTreeIndex
class TestDatabase(unittest.TestCase):
    USER_SCHEMA = ['int', 'var_str', 'var_str', 'var_str', 'var_str', 'var_str', 'int', 'int', 'var_str', 'var_str']

    @classmethod
    def setUpClass(cls):
        # Initialize any setup needed before all tests
        print("Set up.")

        # Create fake users data
        csv_file = "test_fake_users.csv"
        num_rows = 10000
        if not os.path.exists(csv_file):
            utils.generate_fake_data(csv_file, num_rows)  # Makes CSV file.

    def setUp(self):
        # Initialize any setup needed before each test
        self.csv_file = "test_fake_users.csv"
        self.num_rows = 10000
        # Initialize Controller instance
        self.filepath = "test_database.bin"
        self.controller = Controller(self.filepath)  # Makes bin path with nothing in yet.

    def tearDown(self):
        print("Tear down.")
        # Clean up resources after each test
        # For example:
        # if os.path.exists(self.filepath):
        #  os.remove(self.filepath)

    # Insert num_rows of csv into the database and check if everything is correct
    def test_inserts_and_reads(self):
        """"
        Tests the insertion and reading of records into and from the database.

        :param filepath: The path to the database file.
        :param csv_file: The path to the CSV file.
        :param num_rows: The number of rows to insert and read.
        """

        print("===Test Inserts & Reads===")
        csv_data = testutils.read_csv_to_list(self.csv_file, self.num_rows)  # Lists of lists

        # Writing -> Is Ok, binary file gets generated
        start_time = time.time()
        for i, row in enumerate(csv_data[1:]):
            if i == 1:
                print("Row with byte-ID 0: ")
                print(row)
            self.controller.insert(testutils.cast_row_based_on_schema(row, self.USER_SCHEMA), self.USER_SCHEMA)
        self.controller.commit()
        end_time = time.time()
        write_time = end_time - start_time
        writes_per_second = self.num_rows / write_time

        # Reading
        start_time = time.time()
        for i, original_row in enumerate(csv_data[1:]):
            read_row = utils.decode_record(self.controller.read(i), self.USER_SCHEMA)
            print(read_row)
            assert read_row == testutils.cast_row_based_on_schema(original_row, self.USER_SCHEMA)
        self.controller.commit()
        end_time = time.time()
        read_time = end_time - start_time
        reads_per_second = self.num_rows / read_time

        total_time = write_time + read_time

        print(f"Inserted {self.num_rows} records.")
        print(f"Database Size: {os.path.getsize(self.filepath)}")
        print(f"Writes per second: {writes_per_second}")
        print(f"Reads per second: {reads_per_second}")
        print(f"Total completion time: {total_time} seconds")

    # Update the first half of the database and check if everything is correctly updated
    def test_updates(self):
        """
        Tests the updating of records in the database.

        :param filepath: The path to the database file.
        :param csv_file: The path to the CSV file.
        :param num_rows: The number of rows to update.
        """

        print("===Test Updates===")
        controller = Controller(self.filepath)
        faker = Faker()
        df = pd.read_csv(self.csv_file)

        for i in range(self.num_rows // 2):
            action = random.choice(['equal', 'smaller', 'larger'])
            if action == 'smaller':
                df.at[i, 'name'] = df.at[i, 'name'][:-1]
            else:
                df.at[i, 'email'] = faker.name()
                df.at[i, 'email'] = faker.email()
                df.at[i, 'company'] = faker.company()

        start_time = time.time()
        for i in range(self.num_rows // 2):
            controller.update(int(df.iloc[i]['id']), tuple(df.iloc[i]), self.USER_SCHEMA)
        controller.commit()
        end_time = time.time()
        update_time = end_time - start_time
        updates_per_second = (self.num_rows // 2) / update_time

        for i in range(self.num_rows // 2):
            read_row = utils.decode_record(controller.read(i), self.USER_SCHEMA)
            assert read_row == tuple(df.iloc[i])

        print(f"Updated {self.num_rows // 2} records.")
        print(f"Updates per second: {updates_per_second}")
        print(f"Total completion time: {update_time} seconds")

    # Deletes the first half of the database
    def test_deletes(self):
        """
        Tests the deletion of records from the database.

        :param filepath: The path to the database file.
        :param num_rows: The number of rows to delete.
        """

        print("===Test Deletes===")
        controller = Controller(self.filepath)

        start_time = time.time()
        for i in range(self.num_rows // 2):
            controller.delete(i)
        controller.commit()
        end_time = time.time()
        delete_time = end_time - start_time
        deletes_per_second = (self.num_rows // 2) / delete_time

        for i in range(self.num_rows // 2):
            try:
                controller.read(i)
                assert False, "Deleted Record was found!"
            except ValueError:
                pass

        print(f"Deleted {self.num_rows // 2} records.")
        print(f"Deletes per second: {deletes_per_second}")
        print(f"Total completion time: {delete_time} seconds")


def custom_sort(test_case, method):
    test_methods = ['test_inserts_and_reads', 'test_updates', 'test_deletes']
    return test_methods.index(method.__name__)


TestDatabase.sortTestMethodsUsing = custom_sort

# Main method
if __name__ == "__main__":
    unittest.main()
