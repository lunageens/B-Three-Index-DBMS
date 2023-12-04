# * This file initiates the main package

# * Imports
import os
import time
from src.main.database.controller import Controller

# * This file contains the main execution block, where you create instances of the classes and perform database
# operations.

# Main method to demonstrate the insertion operation of one record for a new database in a binary file.
if __name__ == '__main__':
    start = time.time()
    orm = Controller('database.bin')

    record = (0, 'Brian Green', 'michaelfarrell@yahoo.com', '9306399309', 'Cruz LLC', 'Berry Cove', 707, 76486, 'Guam',
              '1981-1-9')
    schema = ['int', 'var_str', 'var_str', 'var_str', 'var_str', 'var_str', 'int', 'int', 'var_str', 'var_str']
    orm.insert(record, schema)

    orm.commit()
    print(f"time taken: {time.time() - start}")
    print(os.path.getsize('database.bin'))
