# * This file initiates the utils package.

from src.main.utils import utils

# Main method to demonstrate the making of a fake database in a csv file.
if __name__ == '__main__':
    filepath = 'fake_users.csv'
    utils.generate_fake_data(filepath, 1000)
