import unittest
import sys

sys.path.append('../')

from db_api import OPIC_DBC

class DB_API_test(unittest.TestCase):

    def setUp():
        dbc = OPIC_DBC('test.db')

