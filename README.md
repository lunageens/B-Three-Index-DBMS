# Database Management System with B+Tree Index

## 1.  Overview
This Python script implements a basic Database Management System (DBMS) using a B+Tree index on a heap file. The code provides functionalities for inserting, updating, reading, and deleting records efficiently.

## 2.   Contents
- Features
- Usage
- Code Structure
- Contributing

## 3.   Features
- B+Tree index for efficient record retrieval.
- Basic CRUD operations on the heap file.
- Page management for organizing and handling records.

## 4.   Usage
To begin, create an instance of the Controller class with the correct filepath. 

<pre>
  orm = Controller('myData.bin')
</pre>

To close the file and save your changes, use the commit changes method of the Controller class.

<pre>
  orm.commit()
</pre>

**Inserting Records:**
To insert a record into the database, use the insert method of the Controller class. Provide the data and schema information.

<pre>
orm.insert(record, schema)
</pre>


**Updating Records:**
To update a record in the database, use the update method of the Controller class. Provide the record's ID, new data (the full adjusted record(s)), and schema information.

<pre>
orm.update(id_, new_data, schema)
</pre>

**Reading Records:**
To read a record from the database, use the read method of the Controller class. Provide the record's ID.

<pre>
result = orm.read(id_)
print(result)
</pre>

**Deleting Records:**
To delete a record from the database, use the delete method of the Controller class. Provide the record's ID.

<pre>
orm.delete(id_)
</pre>

## 5. Code Structure
As always, the distinction between a "main" package and a "test" package typically involves the separation of production code (main) and testing code (test).

### 5.1.  The **main** package
The main package consists out of two sub-packages and a file initiating the package.

The utils and database packages constitute the backbone of the database system. The utils package provides fundamental utilities and constants for efficient data encoding, decoding, and synthetic data generation. On the other hand, the database package encompasses the core functionalities of the database, including page management, record operations, and a B+ tree index implementation. Together, these packages form the essential components for organizing, storing, and manipulating data within the database system.

### _5.1.1.   The **main.database** package_
This database package provides a foundational framework for a file-based database system. It integrates pages, page directories, and a B+ tree index to support basic database operations and optimize record retrieval. Through these components, the system offers a robust solution for managing data in a structured and efficient manner. The system's workflow revolves around the organization of data into pages (clustered in directories), each managed by the `Page` class. The `HeapFile` class oversees the high-level structure, managing multiple page directories to accommodate the growing database. The `Controller` class serves as the gateway for users to perform standard CRUD (Create, Read, Update, Delete) operations on the database. The `BPlusTreeIndex` class brings an efficient indexing mechanism to the database. It implements a B+ tree index, optimizing the retrieval of records based on keys.

#### __init__.py:   _Empty file initiating the database package._

#### **controller**.py:  _Provides an interface for interacting with the database._ 
The `Controller` class provides an interface and calls upon the correct methods in the `HeapFile` class.
Common database operations are implemented:

- Insertion: The _insert_ method takes data and a schema as parameters. It encodes the provided data using the specified schema and inserts the encoded record into the heap file. 
- Updating: The _update_ method takes an ID, new data, and a schema as parameters. It encodes the new data and the ID using their respective schemas. It then updates the record in the heap file with the encoded new data, replacing the existing record with the specified ID. 
- Reading: The _read_ method takes an ID as a parameter. It encodes the ID using its schema and retrieves the corresponding record from the heap file. 
- Deletion: The _delete_ method takes an ID as a parameter. It finds the record in the heap file using the encoded ID and deletes it if found. If the record is not found, it prints a message indicating that the record was not found.

The following other methods can also be found in the controller class:

- Initialization: The class is initialized with a file path, creating an instance of the HeapFile class for file manipulation.
- Committing Changes: The _commit_ method closes the heap file, committing any changes made during the operations.

A limitation to the Controller class right now is that data in the same binary file can be inserted with different schemes, but not read. This is why we should make the scheme a parameter of the constructor of the controller instead of a parameter in the CRUD operations functions. 

#### heap_file.py:  _Implements the heap file and page management._
The `HeapFile` class manages the entire database and organizes data using multiple page directories. It provides methods for handling the insertion, updating, reading, and deletion of records within the database file.

The class manages records through a hierarchical structure, with page directories containing pages, and pages containing records.
- Initialization: The class is initialized with a file path pointing to the database file. If the file exists, it reads the existing data; otherwise, it creates a new PageDirectory. 
- Page Directory Management: The class maintains a list of `PageDirectory` instances, each representing a directory of pages in the database file. The method _read_page_dir(pd)_ reads and returns a specified PageDirectory, loading it if not already in memory.
- File closing: The _close()_ method closes the heap file, writing page directory and page data to the file. It also creates the file if it doesn't exist.

Common database operations are implemented on the record level:

- Insertion: During record insertion with the _insert_record()_ method, the class iterates through page directories and pages, creating new directories or pages when necessary (if full).
- Updating: Record updates are handled by finding the appropriate page and slot ID in the _update_record()_ method, and if there's not enough space, the class tries to find or create a new page.
- Reading: The _find_record(byte_id)_ method finds the page and slot ID for a record with the specified ID. The _read_record(byte_id)_ method reads and returns the record with the specified ID.
- Deletion: Record deletion involves finding the record's location and deleting it from the corresponding page, which is implemented in the _delete_record()_ method.


#### page.py:  _Implements page, record and directory management._
In summary, these classes collectively facilitate the management of database pages, records, and directories, providing essential functionalities for efficient data storage and retrieval. The `Page` class serves as the fundamental unit, while `PageFooter` and `PageDirectory` handle metadata and directory management, respectively. 

The `PageDirectory` class manages a directory of pages and provides methods for finding, creating, and deleting pages. Here's an overview:
- Initialization: The class can be initialized with an optional file path, data, or a current page number.
- Data Pages Operations: The class has methods for finding or creating data pages for insertion, as well as deleting data pages. The _find_page_ method locates a page in the directory based on the page number.
- Record Operations: The _find_record_ method finds a record in the directory based on a byte ID. The _insert_record_ method inserts a record into the directory, managing space constraints.
    - Free Space Update: The _update_free_space_ method updates the free space information for a specific page in the directory.
    - Free Page Listing: The _list_free_pages_ method returns a list of free pages in the directory.

The `Page` class represents a page in a database and plays a crucial role in managing records and a B+ tree index. Here's an overview of its functionalities:
- Initialization: The class can be initialized with existing data or with an empty page. 
- Update Header: The _update_header_ method is responsible for updating the header information within the page. 
- Free Space Calculation: The _free_space_ method calculates and returns the amount of free space available on the page. 
- Slot Offset Calculation: The _calculate_slot_offset_ static method computes the offset of a slot in bytes. 
- Record Operations: The class provides methods for inserting, deleting, reading, updating, and finding records on the page. The _find_record_ method finds a record based on the provided byte_id by iterating through the slot directory and using a B+ tree index if needed.
  - Insertion: the _insert_record_ method inserts a record into the page, ensuring sufficient free space. It writes the record data, updates slot information, and advances the free space pointer.
  - Updating: the _update_record_ method updates a record on the page. It overwrites or compacts the page based on the size of the new record.
  - Reading: the _read_record_ method reads and returns a record from the page based on the provided slot ID.
  - Deletion: the _delete_record_ method deletes a record from the page, marking the corresponding slot as deleted and fixing potential fragmentation.
- Fullness Check: The _is_full_ method checks if the page is full based on its free space. 
- Packed Check: The _is_packed_ method checks if the page is packed, meaning there are no deleted records. 
- Page Compaction: The _compact_page_ method reclaims unused space to limit fragmentation, providing eager compaction when a record is deleted. 
- Data Dump: The _dump_ method prints a comprehensive dump of the data, footer, and records in the page.

The `PageFooter` class represents the footer of a page, containing essential information about free space, the number of slots, and a slot directory. Here's an overview:
- Initialization: The class can be initialized with existing data or with default values. 
- Slot Count: The _slot_count_ method returns the number of slots in the page footer. 
- Data Retrieval: The _data_ method returns the data of the page footer.

This class still has certain limitations. For example, reading from a record that was inserted while the file was open or doesn't exist yet gives an error.

#### **b_plus_tree**.py:  _Contains the B+Tree index implementation._ 
The B+ tree classes maintains balance through splits, ensuring efficient search and insertion operations. The code follows a modular and recursive approach for insertion and search operations. The tree structure is adaptable to handle a dynamic number of keys, optimizing storage and search performance.

Code structure:
A B+ tree is a balanced tree structure commonly used in databases and file systems for efficient indexing and searching. The `BPlusThreeIndex` class represents the top-level of the B+ three. It has an _insert_ method to insert a key with its associated page number and a _search_ method to find a key.  It has two types of nodes: leaf nodes and internal nodes. The keys are stored in the leaf nodes, and internal nodes are used for routing and indexing.
- The `BPlusTreeNode` class represents the leaf nodes. The _insert_ method handles the insertion of a key and page number, and it can _split the node and its child_ if necessary. The split method is responsible for splitting leaf nodes when they become too large. The _search_ method searches for a key in the leaf nodes. There are also methods to _find the index of a key or a child_ in the node and to _sort_ the nodes' keys and children.
- The `BPlusTreeInternalNode` class represents the internal nodes. It inherits from `BPlusTreeNode` class but is used for internal nodes. It _overrides the insert and splitChild method_ to handle internal node-specific operations and splitting.

Key operations explained:
- Insertion: The insert method in both BPlusTreeNode and BPlusTreeInternalNode classes handles key insertion. When a leaf node becomes full, it triggers a split to maintain balance. Internal nodes also perform a split if a child becomes full after insertion.
- Search: The search method in the BPlusTreeNode class searches for a key in the leaf nodes. If the key is not found in a leaf node, the search continues in the appropriate child for internal nodes.
- Splitting mechanism:
  - Leaf Node Split: When a leaf node is full, it is split into two nodes. The keys are redistributed, and a new node is created, maintaining sorted order. A new internal node is created to point to the split leaf nodes.
  - Internal Node Split: Similar to leaf nodes, internal nodes split when a child becomes full after insertion. The split creates a new internal node, redistributes keys and children, and maintains sorted order.

### _5.1.2.   The **main.utils** package:_
The utils package offers crucial utilities and constants for facilitating data management in the broader database framework. Constants in constants.py provide essential parameters, while functions in utils.py handle tasks like encoding, decoding and generating records.

#### __init__.py:   _Empty file initiating the utils package._
The main block of the file demonstrates how to use the data generation function of utils.py. In this example, it generates 100,000 fake user records and saves them to a CSV file named fake_users.csv.

#### utils.py:  _Includes utility functions for data encoding, decoding and generation._
The `utils`.py file provides essential utility functions for encoding and decoding data within the database, along with a practical example of generating synthetic data for testing purposes.

The file includes functions for encoding and decoding entire records, where a record is a collection of fields. These functions are:
- _encode_record_: Encodes a record based on a provided schema, specifying the types of each field.
- _decode_record_: Decodes a record based on the same schema.

On a lower level, the file provides functions for encoding and decoding data fields, which is essential for working with binary data in the context of databases. These functions are crucial for translating data between its human-readable form and the binary representation used within the database. These functions include:
- _encode_var_string_: Encodes a variable-length string.
- _encode_field_: Encodes a field based on its type (e.g., variable-length string, integer, short, byte).
- _decode_field_: Decodes a field based on its type.

Additionally, there is functionality provided for synthetic Data Generation:
- The file provides a function named _generate_fake_data_, which generates synthetic user data for testing purposes. This function utilizes the Faker library to create realistic-looking user records. 

#### constants.py:  _Storing database page (directory) constants_
This code defines crucial constants for managing pages in a database, encompassing both individual pages and a higher-level structure known as a Page Directory. These constants play a pivotal role in determining the size, organization, and functionality of the database pages.

Page Constants:
- PAGE_SIZE: Specifies the size of a typical database page, typically ranging between 512 bytes and 16 kilobytes. This parameter influences the storage and retrieval efficiency of data within the database. 
- OFFSET_SIZE and LENGTH_SIZE: Define the sizes of the offset and length (number of tuples on the page) components within the slot directory of a page. 
- SLOT_ENTRY_SIZE: Represents the total size of a slot entry, including both offset and length components. 
- FREE_SPACE_POINTER_SIZE and NUMBER_SLOTS_SIZE: Indicate the sizes of the free space pointer and the number of slots components within the page footer. 
- FOOTER_SIZE: Represents the total size of the page footer, incorporating the sizes of the free space pointer and the number of slots. 

PageDirectory Constants:
- PAGE_NUM_SIZE and FREE_SPACE_SIZE: Specify the sizes of the page number and free space components within the Page Directory. 
- CACHE_SIZE: Represents the size of the cache used for managing Page Directory entries. This cache helps optimize the retrieval of page-related information within the database.

### 5.2.  The **test** package:
This package contains testing of the different CRUD operations and requires additional attention. The testing is done in two tests:
- The first test, _test_CRUD_singleRecord_, inserts, read, updates and deletes a single record. It also inserts and reads a second one. This test passes.
- In the second test, _test_CRUD_Database_, serves as a practical example or test scenario for showcasing the functionality of the database-related classes and operations within the main package. It creates a fake database in a CSV file. The execution block creates an instance of the Controller class, representing the Object-Relational Mapping (ORM) for a database stored in a file named 'test_CRUD_Database.bin'. The script then defines a sample record and schema, demonstrating the structure of the data to be inserted into the database. It proceeds to use the Controller instance (orm) to insert the record into the database, followed by committing the changes. Finally, the code prints the time taken for the database operation and the size of the resulting 'database.bin' file. Then, the CRUD operations are preformed. This test fails and we have yet to find the bug in our code. Note that if you want to correctly rerun this test, u should remove the output bin and csv file every time. 

## 6. Contributing
A project of Ruben for the course Database Systems of professor Len Feremans from the University of Antwerp.
