# * Imports
from src.main.utils.constants import *
from src.main.database.bplus_three import BPlusTreeIndex
from typing import Optional


# * The Page class represents a page in the database, containing records and a B+ tree index. It provides methods for
# inserting, deleting, and updating records.
class Page:
    # Initialization of a Page instance with optional existing data
    def __init__(self, data=None):
        self.data = bytearray(PAGE_SIZE) if data is None else data
        self.page_footer = PageFooter(self.data)
        page_footer_data = self.page_footer.data()
        self.data[-len(page_footer_data):] = page_footer_data
        self.bplus_tree_index = BPlusTreeIndex()

    # Updates the header information in the page
    def update_header(self):
        page_footer_data = self.page_footer.data()
        self.data[-len(page_footer_data):] = page_footer_data

    # Calculates and returns the free space available on the page
    def free_space(self):
        # 512 - 100 - (x * 8) - 4 = 508
        # Page header grows from bottom up, records grow top down.
        # Free space pointer - space occupied by page header - 4 bytes for free space pointer
        return PAGE_SIZE - self.page_footer.free_space_pointer - (
                len(self.page_footer.slot_dir) * SLOT_ENTRY_SIZE) - FREE_SPACE_POINTER_SIZE - NUMBER_SLOTS_SIZE

    #  Calculate the offset of a slot in bytes
    @staticmethod
    def calculate_slot_offset(slot_id):
        """
        Calculate the offset of a slot in bytes, this is the location it starts, so write to right to left.

        :param slot_id: Slot id
        :return: Offset in bytes
        """
        return (PAGE_SIZE - FREE_SPACE_POINTER_SIZE * 2) - (SLOT_ENTRY_SIZE * (slot_id + 1))

    # Inserts a record into the page
    def insert_record(self, record: bytearray):
        """
        If there is not enough free space -> try to compact data, and use this free space, otherwise record can't be stored
        First check if there is a slot with 0 as length, to overwrite this
        :param record:
        :return:
        """
        needed_space = len(record) + SLOT_ENTRY_SIZE
        if needed_space > self.free_space():
            return False

        # Write data
        self.data[self.page_footer.free_space_pointer:self.page_footer.free_space_pointer + len(record)] = record

        # Check if page is packed, meaning no deleted records
        if self.is_packed():
            index = self.page_footer.slot_count()
        else:
            index = 0
            for i, (_, length) in enumerate(self.page_footer.slot_dir):
                if length == 0:
                    index = i

        # Update slots
        new_slot_offset = Page.calculate_slot_offset(index)

        # (offset, length)
        self.data[new_slot_offset: new_slot_offset + OFFSET_SIZE] = self.page_footer.free_space_pointer.to_bytes(
            OFFSET_SIZE, 'little')
        self.data[new_slot_offset + OFFSET_SIZE: new_slot_offset + SLOT_ENTRY_SIZE] = len(record).to_bytes(LENGTH_SIZE,
                                                                                                           'little')

        # Update page footer
        if self.is_packed():
            self.page_footer.slot_dir.append((self.page_footer.free_space_pointer, len(record)))
        else:
            self.page_footer.slot_dir[index] = (self.page_footer.free_space_pointer, len(record))

        # Update free space pointer
        self.page_footer.free_space_pointer += len(record)
        self.update_header()

        return True

        key = record[:4]  # Take first bytes as key
        page_number = record[-4:]  # Take last bytes as pagenumber
        self.bplus_tree_index.insert(key, page_number)

    # Deletes a record from the page
    def delete_record(self, slot_id):
        offset, length = self.page_footer.slot_dir[slot_id]
        self.page_footer.slot_dir[slot_id] = (offset, 0)
        new_slot_offset = Page.calculate_slot_offset(slot_id)
        number = 0
        self.data[new_slot_offset + OFFSET_SIZE:new_slot_offset + SLOT_ENTRY_SIZE] = number.to_bytes(LENGTH_SIZE,
                                                                                                     'little')
        # Fix fragmentation
        self.compact_page()

    #  Reads and returns a record from the page
    def read_record(self, slot_id):
        offset, length = self.page_footer.slot_dir[slot_id]
        return self.data[offset: offset + length]

    # Updates a record on the page
    def update_record(self, slot_id, new_record):
        offset, length = self.page_footer.slot_dir[slot_id]
        # If new record size is equal, just overwrite
        if len(new_record) == length:
            self.data[offset:offset + length] = new_record
            return True
        # If new record is smaller, we need to compact the page to avoid fragmentation
        elif len(new_record) < length:
            self.data[offset:offset + len(new_record)] = new_record
            new_slot_offset = Page.calculate_slot_offset(slot_id)
            self.page_footer.slot_dir[slot_id] = (offset, len(new_record))
            self.data[new_slot_offset + OFFSET_SIZE:new_slot_offset + SLOT_ENTRY_SIZE] = len(new_record).to_bytes(
                LENGTH_SIZE, 'little')
            self.compact_page()
            return True
        # New record is lager, we can just insert the record
        else:
            # Delete record, length will be set to -1
            self.delete_record(slot_id)
            # If returns True, enough free space on the page and slot_id stays the same, else we need to find a new page
            return self.insert_record(new_record)

    # Finds a record based on the provided byte_id
    def find_record(self, byte_id: bytearray) -> int:
        for slot_id, (offset, length) in enumerate(self.page_footer.slot_dir):
            # some record, we assume the first field is the id and an int
            record = self.data[offset: offset + length]
            if byte_id == record[:4]:
                return slot_id
            key = byte_id[:4]  # Take first 4 bytes as key
            page_number = self.bplus_tree_index.search(key)
            return page_number

    # Checks if the page is full
    def is_full(self):
        return self.free_space() <= 0

    # Checks if the page is packed, meaning no deleted records
    def is_packed(self):
        """
        Check if page is packed, meaning no deleted records.
        """
        return all(length != 0 for offset, length in self.page_footer.slot_dir)

    # Reclaims unused space to limit fragmentation
    def compact_page(self):
        """
        Reclaim unused space so that records are contiguous and limit fragmentation.

        Eager -> compact page when a record is deleted (we do this)
        Lazy -> compact page when page is full
        """
        write_ptr = 0

        for i, (offset, length) in sorted(enumerate(self.page_footer.slot_dir), key=lambda x: x[1][0]):
            # Skip deleted records
            if length != 0:
                if offset != write_ptr:
                    self.data[write_ptr:write_ptr + length] = self.data[offset:offset + length]
                self.page_footer.slot_dir[i] = (write_ptr, length)
                # Update slots in bytes
                new_slot_offset = Page.calculate_slot_offset(i)
                self.data[new_slot_offset: new_slot_offset + OFFSET_SIZE] = write_ptr.to_bytes(OFFSET_SIZE, 'little')
                self.data[new_slot_offset + OFFSET_SIZE: new_slot_offset + SLOT_ENTRY_SIZE] = length.to_bytes(
                    LENGTH_SIZE, 'little')
                write_ptr += length

        self.page_footer.free_space_pointer = write_ptr
        self.update_header()

    # Dumps information about the data, footer, and records in the page
    def dump(self):
        print("=== Data Byte Dump ===")

        for i, byte in enumerate(self.data):
            if i % 16 == 0:
                print()
            print(f"{byte:02x} ", end='')
        print("\n")

        print("=== Footer Dump ===")
        print(f"Free Space Pointer: {self.page_footer.free_space_pointer}")
        print(
            f"Free Space Pointer (bytes): {int.from_bytes(self.data[-FREE_SPACE_POINTER_SIZE:], 'little')}")

        print(f"Number of slots: {self.page_footer.slot_count()}")
        print(
            f"Number of slots (bytes): {int.from_bytes(self.data[PAGE_SIZE - FREE_SPACE_POINTER_SIZE * 2:PAGE_SIZE - FREE_SPACE_POINTER_SIZE], 'little')}")
        print("\n")

        print("=== Record Dump ===")
        print("Slot: offset | length")
        print("==========")
        for i, (offset, length) in enumerate(self.page_footer.slot_dir):
            if offset == -1:
                print(f"Record {i}: Deleted")
                continue
            record_bytes = self.data[offset:offset + length]
            print(f"Slot {i}: {offset} | {length}")

            slot_offset = -(FREE_SPACE_POINTER_SIZE * 2) - (i + 1) * SLOT_ENTRY_SIZE
            record_offset = self.data[slot_offset: slot_offset + OFFSET_SIZE]
            record_length = self.data[slot_offset + OFFSET_SIZE: slot_offset + SLOT_ENTRY_SIZE]
            print(
                f"Slot (bytes): {int.from_bytes(record_offset, 'little')} | {int.from_bytes(record_length, 'little')}")
            print(f"Record {i} (bytes): {record_bytes}")
            print(f"Record {i}: {int.from_bytes(record_bytes, 'little')}")


# * The PageFooter class represents the footer of a page, containing information about free space, the number of
# slots, and a slot directory.
class PageFooter:
    # Initialization of a PageFooter instance with optional existing data
    def __init__(self, data: bytearray = None):
        data = bytearray(PAGE_SIZE) if data is None else data
        # Pointer to free space
        self.free_space_pointer = int.from_bytes(data[-FREE_SPACE_POINTER_SIZE:], 'little')
        # Number of slots
        slot_count = int.from_bytes(data[-FOOTER_SIZE:-FREE_SPACE_POINTER_SIZE], 'little')

        # Contains pairs (offset to beginning of record, length of record), if length == 0, then record is deleted
        self.slot_dir = []
        for i in range(slot_count):
            slot = data[-FOOTER_SIZE - (i + 1) * SLOT_ENTRY_SIZE:-FOOTER_SIZE - i * SLOT_ENTRY_SIZE]
            offset, length = slot[:OFFSET_SIZE], slot[LENGTH_SIZE:]
            self.slot_dir.append((int.from_bytes(offset, 'little'), int.from_bytes(length, 'little')))

    # Returns the number of slots, written on the page footer
    def slot_count(self):
        return len(self.slot_dir)

    # Returns the data of the page footer
    def data(self) -> bytearray:
        return bytearray(
            len(self.slot_dir).to_bytes(NUMBER_SLOTS_SIZE, byteorder='little') + self.free_space_pointer.to_bytes(
                FREE_SPACE_POINTER_SIZE, byteorder='little'))


# * The PageDirectory class manages a directory of pages and provides methods for finding, creating, and deleting pages.
class PageDirectory(Page):
    # Initialization of a PageDirectory instance with optional existing data
    def __init__(self, file_path: str = None, data: bytearray = None, current_number: int = None):
        self.data = bytearray(PAGE_SIZE) if data is None else data
        self.pages = {}  # Dictionary to store page information
        self.file_path = file_path
        super().__init__(self.data)
        # Information about page directories
        if data is None and current_number is None:
            self.pd_number = 0
            self.next_dir = 0
            byte_array = bytearray(
                self.pd_number.to_bytes(PAGE_NUM_SIZE, 'little') + self.next_dir.to_bytes(FREE_SPACE_SIZE, 'little'))
            # First slot points to record --> (current_pd_number, next_pd_number)
            super().insert_record(byte_array)
        elif current_number is not None:
            self.pd_number = current_number + 1
            self.next_dir = 0
            byte_array = bytearray(
                self.pd_number.to_bytes(PAGE_NUM_SIZE, 'little') + self.next_dir.to_bytes(FREE_SPACE_SIZE, 'little'))
            # First slot points to record --> (current_pd_number, next_pd_number)
            super().insert_record(byte_array)
        else:
            record = super().read_record(0)
            self.pd_number, self.next_dir = int.from_bytes(record[:PAGE_NUM_SIZE], 'little'), int.from_bytes(
                record[FREE_SPACE_SIZE:], 'little')

    # Finds a page in the directory based on the page number
    def find_page(self, page_number) -> Optional[Page]:

        if page_number in self.pages:
            return self.pages[page_number]

        for offset, length in self.page_footer.slot_dir[1:]:
            data = self.data[offset:offset + length]
            if int.from_bytes(data[:PAGE_NUM_SIZE], 'little') == page_number:
                # TODO - reading from record that was inserted while file was open and doesn't exist yet gives error
                assert self.file_path is not None
                with open(self.file_path, "rb") as db:
                    db.seek(page_number * PAGE_SIZE)
                    page = Page(bytearray(db.read(PAGE_SIZE)))
                    self.pages[page_number] = page
                    return page

    # Finds a record in the directory based on the byte_id
    def find_record(self, byte_id: bytearray) -> (int, int):
        for offset, length in self.page_footer.slot_dir[1:]:
            page_number = int.from_bytes(self.data[offset: offset + PAGE_NUM_SIZE], 'little')
            page: Page = self.find_page(page_number)
            record = page.find_record(byte_id)
            if record is not None:
                return page, record
        return False

    # Finds or creates a data page for insertion of a record
    def find_or_create_data_page_for_insert(self, needed_space):

        page_num = 0
        # TODO NOW - minus one since first slot references to page dir. info
        for slot_id in range(self.page_footer.slot_count() - 1):
            # loop over slot, read in tuple(record) -> should contain (page num, free space)
            # TODO NOW - increase slot_id with 1 since first slot will reference page dir. info
            offset, length = self.page_footer.slot_dir[slot_id + 1]
            record = self.data[offset: offset + length]
            page_num, free_space = int.from_bytes(record[:PAGE_NUM_SIZE], 'little'), int.from_bytes(
                record[FREE_SPACE_SIZE:], 'little')
            if needed_space <= free_space:
                break

        else:
            # no page found make new one
            # save new page in footer, TODO check if there is still space available to link to next page directory
            # Check if there is enough free space in page dir. --> (page_nr, free_space) + slot size
            if (PAGE_NUM_SIZE + FREE_SPACE_SIZE) + SLOT_ENTRY_SIZE > self.free_space():
                return False

            page = Page()
            # Find the max. current page number
            page_num = int.from_bytes(self.read_record(len(self.page_footer.slot_dir) - 1)[:PAGE_NUM_SIZE],
                                      'little') + 1
            byte_array = bytearray(
                page_num.to_bytes(PAGE_NUM_SIZE, 'little') + page.free_space().to_bytes(FREE_SPACE_SIZE, 'little'))
            # add data page info to page directory
            super().insert_record(byte_array)
            self.pages[page_num] = page
            return True

        # Gets executed when space is left in Page
        assert self.file_path is not None
        with open(self.file_path, "rb") as db:
            db.seek(page_num * PAGE_SIZE)
            page = Page(bytearray(db.read(PAGE_SIZE)))

        self.pages[page_num] = page
        return True

    # Deletes a data page from the directory
    def delete_data_page(self, page_number):
        # Mark a data page as free in the directory
        if page_number in self.pages:
            self.pages[page_number]['status'] = 'free'
            print(f"Deleted data page {page_number}")

    # Inserts a record into the directory
    def insert_record(self, data: bytearray):
        for nr, page in self.pages.items():
            if page.is_full():
                # self.full_pages.append(self.pages.pop(page_number))
                continue

            elif page.insert_record(data):
                self.update_free_space(nr, page.free_space())
                return True  # Tuple written successfully
        # All existing pages are full, create a new page and write the tuple
        if not self.find_or_create_data_page_for_insert(len(data) + SLOT_ENTRY_SIZE):
            return False
        return self.insert_record(data)

    # Updates the free space information for a page in the directory
    def update_free_space(self, page_nr, free_space):
        # TODO NOW - Calculate relative page_nr inside page dir.
        page_nr = page_nr - self.pd_number
        offset, length = self.page_footer.slot_dir[page_nr]
        self.data[offset + PAGE_NUM_SIZE:offset + PAGE_NUM_SIZE + FREE_SPACE_SIZE] = free_space.to_bytes(
            FREE_SPACE_SIZE, 'little')

    # Returns a list of free pages in the directory
    def list_free_pages(self):
        return [page for page, info in self.pages.items() if info['status'] == 'free']
