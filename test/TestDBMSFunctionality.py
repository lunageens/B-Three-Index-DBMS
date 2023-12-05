import os
import time
import unittest

import src.main.utils.utils as utils
import testutils as testutils
from src.main.database.controller import Controller  # Import your Controller class


class TestDBMSFunctionality(unittest.TestCase):

    # * This test demonstrates the CRUD operations on one record for a new database in a binary file.
    def test_CRUD_singleRecord(self):
        orm = Controller('test_CRUD_singleRecord.bin')
        schema = ['int', 'var_str', 'var_str', 'var_str', 'var_str', 'var_str', 'int', 'int', 'var_str', 'var_str']

        # Insert
        start = time.time()
        record = (
        0, 'Brian Green', 'michaelfarrell@yahoo.com', '9306399309', 'Cruz LLC', 'Berry Cove', 707, 76486, 'Guam',
        '1981-1-9')
        orm.insert(record, schema)
        orm.commit()
        print(f"time taken for insertion: {time.time() - start}")
        print("size of binary file: " + str(os.path.getsize('test_CRUD_singleRecord.bin')))

        # Read
        start = time.time()
        row = orm.read(0)
        assert row == record
        print("Row: ")
        print(row)
        print(f"time taken for reading: {time.time() - start}")

        # Update
        adjusted_record = (
            0, 'John Green', 'michaelfarrell@yahoo.com', '9306399309', 'Cruz LLC', 'Berry Cove', 707, 76486, 'Guam',
            '1981-1-9')
        orm.update(0, adjusted_record, schema)
        orm.commit()
        row = orm.read(0)
        assert row == adjusted_record
        print("Adjusted row: ")
        print(row)

        # Delete
        orm.delete(0)
        try:
            orm.read(0)  # Should receive Value error for this
            assert False, "Expected ValueError not raised. Deleted ID still exists."
        except ValueError as e:
            print("The deleted row was not found anymore.")

        # Try inserting a second row
        second_record = (
            1, 'Luna Green', 'michaelfarrell@yahoo.com', '9306399309', 'Cruz LLC', 'Berry Cove', 707, 76486, 'Guam',
            '1981-1-9')
        orm.insert(second_record, schema)
        orm.commit()
        row = orm.read(1)
        assert row == second_record
        print("Second Row: ")
        print(row)

        os.remove('test_CRUD_singleRecord.bin')

    def test_CRUD_Database(self):
        csv_file_path = 'test_CRUD_Database.csv'
        binary_file_path = 'test_CRUD_Database.bin'
        schema = ['int', 'var_str', 'var_str', 'var_str', 'var_str', 'var_str', 'int', 'int', 'var_str', 'var_str']

        # * 0. Set up the initial conditions for your tests, e.g., create a test database file
        print("Setting up tests.")
        # Generate test data for initial testing and save it to CSV
        utils.generate_fake_data(csv_file_path, 10)  # TODO Make more rows
        # Read data from CSV
        csv_data = testutils.read_csv_to_list(csv_file_path)
        csv_data = csv_data[1:]  # Not the headers
        assert csv_data is not None  # Make sure that we read in the data correctly
        # Create n instance of the Controller class with the correct binary file path
        orm = Controller(binary_file_path)

        # * 1. Test the insert method of the Controller class
        print("Testing insertion and reading.")
        # Insert data from CSV to binary file
        for row in csv_data:
            record = testutils.cast_row_based_on_schema(row, schema)
            orm.insert(record, schema)

        # Commit changes and close the binary file
        orm.commit()

        # Verify that each record from CSV can be read from the binary file
        for i, expected_row in enumerate(csv_data):
            expected_record = testutils.cast_row_based_on_schema(expected_row, schema)
            print(i)
            print(expected_record) # ? Heel raar - crasht pas vanaf de tweede? Binary file bevat wel altijd steeds de records
            retrieved_record = orm.read(i)
            self.assertEqual(retrieved_record, expected_record)
        print("Passed the insertion and reading test.")

        # * 2. Test the update method of the Controller class
        print("Testing updating and reading.")
        # Update the first record in the binary file
        record_id = 0
        updated_data = (
            0, 'Updated Name', 'updated.email@example.com', '9876543210', 'Updated Corp', 'Updated St', 999, 12345,
            'Updated Country', '2000-02-02')
        orm.update(record_id, updated_data, schema)
        # Commit changes and close the binary file
        orm.commit()
        # Verify that the updated record is present in the database
        retrieved_record = orm.read(record_id)
        self.assertEqual(retrieved_record, updated_data)

        # * 3. Test the delete method of the Controller class
        print("Testing deletion and reading.")
        # Delete a record in the binary file
        record_id = 1
        orm.delete(record_id)
        # Commit changes and close the binary file
        orm.commit()
        # Verify that the record is deleted from the database
        retrieved_record = orm.read(record_id)
        self.assertIsNone(retrieved_record)

        # * 4. Clean up after each test, e.g., close the database and commit changes
        orm.commit()
        os.remove(csv_file_path)
        os.remove(binary_file_path)

if __name__ == '__main__':
    unittest.main()
