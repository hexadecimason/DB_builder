import unittest
import sys
import shutil

sys.path.append('../')

from db_api import OPIC_DBC

class DB_API_test(unittest.TestCase):

    def setUp(self):

        #shutil.copy(src = 'test_bk.db', dst = 'test.db')
        self.dbc = OPIC_DBC('test.db')


    def test_initialization(self):
        """
        Tests that initialization with a bad filename sets the conneciton to None.    
        """

        #case: file not found, should return None
        con = OPIC_DBC('bad_filename.db')
        self.assertEquals(con, None)

        
    def test_auth(self):

        print("testing write permissions")
        
        self.dbc.add_well({'api':0})     

        
    # TODO: ensure commits happen manually, not auto
    def test_comittal(self):

        print("testing manual commit")

        # create conneciton, add without committing, and close
        # then, reconnect and query


    def test_verify_keys(self):
        tup = ('A', 'B', 'C')
        l = ['A', 'B', 'C']
        l_2 = [1, 2, 3]

        print('##### TESTING: verify_keys()')

        self.assertTrue(verify_keys(tup, l))
        self.assertFalse(verify_keys(tup, l_2))
        

    def test_add_well(self):
        w1 = {'api':111111, 'operator':"Big Hoss' Drlg Co",
                'lease':'Toadlick', 'well_num':42}

        print('##### TESTING: add_well()')

        # should add as expected
        print(self.dbc.add_well(w1))
        
        # FAILURE CASES
        
        # empty api, should fail PK constraint
        self.assertRaises(Exception, dbc.add_well({'api':None}))

        # API already present, FK constraint fails
        self.assertRaises(Exception, dbc.add_well(w1))
        

    def test_add_file(self):
        f1 = {'file_num':1000000, 'collection':'SOME COLLECTION',
            'sample_type':'SLAB'}

        print('##### TESTING: add_file()')

        # should add as expected
        print(self.dbc.add_file(111111, f1))

        # FAILURE CASES
        
        # Primary Key failure
        self.assertRaises(Exception, self.dbc.add_file(111111, f1))
        
        # No file num
        self.assertRaises(Exception, self.dbc.add_file({'file_num':None,'collection':'some colleciton'}))
        
        # No collection
        self.assertRaises(Exception, self.dbc.add_file({'file_num':222222,'collection':None}))


    def test_add_box(self):
        b1 = {'box_num':'Q-1', 'top':0, 'bottom':100000, 'formation':'NONDIFFERENTIABLE MANIFOLD'}

        print('##### TESTING: add_box()')

        # should add as expected
        print(self.dbc.add_box(1000000, b1))

        # FAILURE CASES
        
        # Primary Key failure
        self.assertRaises(Exception, self.dbc.add_box(1000000, b1))

        # no box number
        self.assertRaises(Exception, self.dbc.add_box(1000000, {'box_num':None}))

        
    # TODO: TEST MODIFICATION FUNCITONS
    def test_modify_well(self):

        print('##### TESTING: modify_well()')
         

    def test_modify_file(self):
        
        print('##### TESTING: modify_file()')
        

    def test_modify_box(self):

        print('##### TESTING: modify_box()')
        
    # spit out results of queries
    def test_logging(self):

        print('##### testing logs')
        res = self.dbc.connection.execute('SELECT * FROM Q_LOG')
        

    
