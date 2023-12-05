# * Imports
import struct
from faker import Faker
import pandas as pd
import random
from typing import List


# Encodes a variable-length string
def encode_var_string(s: str):
    return [len(s)] + list(s.encode('UTF-8'))


# Encodes a field based on its type.
def encode_field(value, field_type: str):
    if field_type == 'var_str':
        return encode_var_string(value)
    elif field_type == 'int':
        # 4-byte int
        return list(struct.pack("<I", value))
    elif field_type == 'short':
        # 2-byte int
        return list(struct.pack("<H", value))
    elif field_type == 'byte':
        # 1-byte int
        return [value]
    else:
        raise ValueError(f"Unknown field_type {field_type}")


# Decodes a field based on its type.
def decode_field(byte_array, start_idx, field_type):
    if field_type == 'var_str':
        str_len = byte_array[start_idx]
        return str(byte_array[start_idx + 1: start_idx + 1 + str_len], 'utf-8'), start_idx + 1 + str_len
    elif field_type == 'int':
        return struct.unpack("<I", byte_array[start_idx:start_idx + 4])[0], start_idx + 4
    elif field_type == 'short':
        return struct.unpack("<H", byte_array[start_idx:start_idx + 2])[0], start_idx + 2
    elif field_type == 'byte':
        return byte_array[start_idx], start_idx + 1
    else:
        raise ValueError(f"Unknown field_type {field_type}")


# Encodes a record based on the provided schema.
def encode_record(record, schema: List[str]):
    encoded_fields = []
    for value, field_type in zip(record, schema):
        encoded_fields.extend(encode_field(value, field_type))
    return bytearray(encoded_fields)


# Decodes a record based on the provided schema.
def decode_record(byte_array, schema: List[str]):
    decoded_fields = []
    start_idx = 0
    for field_type in schema:
        value, start_idx = decode_field(byte_array, start_idx, field_type)
        decoded_fields.append(value)
    return tuple(decoded_fields)


# TODO Should this not be in testutils?

#  Generates fake user data and saves the data to a CSV file.
def generate_fake_data(file_path: str, rows: int):
    user_columns = ['id', 'name', 'email', 'phone', 'company', 'street', 'street_number', 'zipcode', 'country',
                    'birthdate']

    # ! I removed the new initiation of new B+ Three, which was what was used before to call the insert method on.
    # The initiation of the BPlusThree should happen automatically:
    # When a controller is created: makes heap file in its constructor.
    # In the constructor of the heap file, it makes list of page directors and initiates one page directory if needed
    # In the constructor page directory, he inherits from page objects that get made.
    # In constructor page automatically a new b plus three index is made.
    # On top of that, we just make csv file in this method. No need for B+ Three.

    users = []
    fake = Faker()
    for i in range(rows):
        user = [i, fake.name(), fake.ascii_email(), fake.basic_phone_number(), fake.company(), fake.street_name(),
                random.randint(1, 1000), fake.zipcode(), fake.country(),
                f'{random.randint(1970, 2005)}-{random.randint(1, 12)}-{random.randint(1, 28)}']

        # we store the data in the BPlusTreeIndex
        users.append(user)

    # We save the data into a csv
    df = pd.DataFrame(users, columns=user_columns)
    df.to_csv(file_path, index=False)
