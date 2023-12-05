from typing import List
import csv

# Casts a provided row based on the provided schema. From each value as a string in the list row to tuple with
# correct datatypes.
def cast_row_based_on_schema(row, schema):
    """
    Casts a row based on the provided schema.

    :param row: The row to cast.
    :param schema: The schema to use for casting.
    :return: A tuple representing the casted row.
    """
    return tuple(int(val) if dt == 'int' else val for val, dt in zip(row, schema))

# Reads a CSV file and returns its contents as a list.
def read_csv_to_list(filepath: str, num_rows: int = None) -> List[List[str]]:
    """
   Reads a CSV file and returns its contents as a list.

   :param filepath: The path to the CSV file.
   :param num_rows: The number of rows to read.
   :return: A list containing the CSV data.
   """
    with open(filepath, 'r') as f:
        return list(csv.reader(f))[:num_rows]