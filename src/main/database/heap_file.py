# * Imports
from src.main.database.page import PageDirectory
from src.main.utils.constants import *
import src.main.utils.utils as utils
import os


# * This class is responsible for represents the entire database and manages multiple page directories.
class HeapFile:
    # Initializes the HeapFile with the given file path and loads existing data or creates a new PageDirectory.
    def __init__(self, file_path):
        self.file_path = file_path
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as db:
                pd = PageDirectory(file_path=file_path, data=bytearray(db.read(PAGE_SIZE)))
        else:
            pd = PageDirectory(file_path) # !Changed this so it also has filepath as parameter
        self.page_directories: list[PageDirectory] = [pd]

    # Reads and returns the specified PageDirectory, loading it if not already in memory.
    def read_page_dir(self, pd: PageDirectory) -> PageDirectory:
        if new_pd := list(filter(lambda pgd: pgd.pd_number == pd.next_dir, self.page_directories)):
            return new_pd[0]

        with open(self.file_path, 'rb') as db:
            db.seek(pd.next_dir * PAGE_SIZE)
            new_pd = PageDirectory(file_path=self.file_path, data=bytearray(db.read(PAGE_SIZE)))
        self.page_directories.append(new_pd)
        return new_pd

    # Deletes the record with the specified page ID and slot ID.
    def delete_record(self, page_id, slot_id):
        if page_id < len(self.pages):
            self.pages[page_id].delete_record(slot_id)

    # Updates the record with the specified ID, replacing it with the given data.
    def update_record(self, byte_id: bytearray, data):
        page, slot_id = self.find_record(byte_id)
        if page.update_record(slot_id, data):
            return True
        else:
            # Not enough free space on page, try to find a new page
            self.page_directories[0].insert_record(data)

    # Inserts a record into the database, handling page directory and page creation as needed.
    def insert_record(self, data):
        pd: PageDirectory = self.page_directories[0]

        # Iterate over all page dir., if full move to the next one
        while not (inserted := pd.insert_record(data)) and pd.next_dir != 0:
            pd = self.read_page_dir(pd)

        # If last dir. is full, create new one
        if not inserted:
            # Find the max. current page number
            max_page_nr = int.from_bytes(pd.read_record(len(pd.page_footer.slot_dir) - 1)[:PAGE_NUM_SIZE], 'little')
            # Create new page directory
            new_pd = PageDirectory(file_path=self.file_path, current_number=max_page_nr)
            pd.next_dir = new_pd.pd_number
            # (current_pd_number, next_pd_number)
            pd.data[PAGE_NUM_SIZE:PAGE_NUM_SIZE + FREE_SPACE_SIZE] = pd.next_dir.to_bytes(FREE_SPACE_SIZE, 'little')
            self.page_directories.append(new_pd)
            return new_pd.insert_record(data)
        return True

    # Finds and returns the page and slot ID for the record with the specified ID.
    def find_record(self, byte_id: bytearray) -> (int, int):
        pd: PageDirectory = self.page_directories[0]

        while True:
            if result := pd.find_record(byte_id):
                return result
            if pd.next_dir == 0:
                break
            pd = self.read_page_dir(pd)

        return None, None

    # Reads and returns the record with the specified ID.
    def read_record(self, byte_id: bytearray):
        page, slot_id = self.find_record(byte_id)
        if page is None:
            raise ValueError('Record with this ID is not found!')
        return page.read_record(slot_id)

    # Finds and returns the page with the specified page number.
    def find_page(self, page_number):
        for page_directory in self.page_directories:
            if page := page_directory.find_page(page_number):
                return page

    # Closes the heap file, writing page directory and page data to the file.
    # Creates the file if it doesn't exist.
    def close(self):
        # Create file if it doesn't exist
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'wb') as file:
                file.close()
        print("Closing file with committed changes.")
        with open(self.file_path, 'r+b') as file:
            for page_dir in self.page_directories:
                file.seek(page_dir.pd_number * PAGE_SIZE)
                file.write(page_dir.data)
                for page_nr, page in page_dir.pages.items():
                    file.seek(page_nr * PAGE_SIZE)
                    file.write(page.data)
