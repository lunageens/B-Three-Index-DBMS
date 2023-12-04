# * Imports
from src.main.database.heap_file import HeapFile
from src.main.utils import utils
from typing import List

# TODO Make schema a initialized attribute of the controller class instead of with Insert. Otherwise data from
#  different schemas will be in one file.

# * The Controller class acts as an interface for interacting with the database. It provides methods for inserting,
# updating, reading, and deleting records.
class Controller:
    # Initialize the Controller with a HeapFile instance for file manipulation.
    def __init__(self, filepath):
        self.schema = None
        self.heap_file = HeapFile(filepath)
        self.filepath = filepath

    # Insert a record into the heap file by encoding the data using the provided schema.
    def insert(self, data, schema: List[str]):
        self.schema = schema  # ! Wat als er verschillende schemas worden gebruikt?
        self.heap_file.insert_record(utils.encode_record(data, schema))

    # Update a record identified by the given id by encoding the new data and id using their respective schemas.
    def update(self, id_: int, data, schema: List[str]):
        self.heap_file.update_record(utils.encode_record([id_], ['int']), utils.encode_record(data, schema))

    # Read a record identified by the given id by encoding the id using its schema.
    def read(self, id_: int):
        byte_id = utils.encode_record([id_], ['int'])
        return utils.decode_record(self.heap_file.read_record(byte_id), self.schema)  # ! Hier decode aan toegevoegd

    # Find the record in the heap file using the encoded id, and delete it if found.
    def delete(self, id_: int):
        (page, slot_id) = self.heap_file.find_record(utils.encode_record([id_], ['int']))
        if page is None:  # Print a message if the record is not found.
            print('Record not found!')
            return
        page.delete_record(slot_id)

    # Close the heap file, committing any changes made.
    def commit(self):
        self.heap_file.close()
