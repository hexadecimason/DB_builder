import sqlite3 as sq
import numpy as np
import pandas as pd
import os

class OPIC_DBC():
    """
    A class for creating and using OPIC well database conenctions.

    This class is based around abstracted methods for searching,
    adding, deleting and updating. Using this class requires no SQL,
    although it is possible using the connection cursor instance within the class.

    Static Variables:
    ------------------
    well_attr,
    file_attr,
    box_attr,
    well_file_attr: tuples of the names of attributes in each relation 
                    of the database. These are used for verification.

    (CAPS VALUES): queries for adding, searching, and deleting. Updating is more
                   complex and the logic of query formatting is inside such functions.

    Static Methods:
    ---------------
    verify_keys(const_tuple, checklist)

    Instance Variables:
    -------------------
    connection: the sqlite3 connection
    cursor:     the sqlite3 cursor
    write:      manual control over write permissions for this ODBC instance

    Instance Methods:
    ---------------
    state_check()
    end_edit()
    grid_search(type, value, df_out = False)
    modify_box(file_num, box_num, vlauedict)
    modify_file(file_num, valuedict)
    modify_well(api, valuedict)
    add_box(file_num, box_num, valuedict)
    add_file(file_num, vlauedict)
    add_well(api, valuedict)
    remove_box(file_num, box_num)
    remove_file(file_num)
    remove_well(api)
    """

    well_attr = ('api', 'operator', 'lease', 'well_num', 'sec', 'twn', 'twn_d', 'rng', 'rng_d',
                'qq', 'lat', 'long', 'county', 'state', 'field')
    file_attr = ('file_num', 'collection', 'sample_type', 'box_count', 'box_type', 'diameter', 'location')
    box_attr = ('file_num', 'box_num', 'top', 'bottom', 'formation', 'condition', 'comments')
    well_file_attr = ('api', 'file_num')
    
    ADD_WELL = "INSERT INTO Well VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    ADD_FILE = "INSERT INTO File VALUES (?,?,?,?,?, ?,?)"
    ADD_BOX = "INSERT INTO Box VALUES (?,?,?,?,?, ?,?)"

    DELETE_WELL = "DELETE FROM Well WHERE api = ?"
    DELETE_FILE = "DELETE FROM File WHERE file_num = ?"
    DELETE_BOX = "DELETE FROM Box WHERE file_num = ? AND box_num = ?"

    GRID_QUERY  = '''SELECT * FROM xl_grid WHERE '''

    def __init__(self, db_path, write = False):
        """
        Begins with a file path to the database, attempts to connect.
        Failure sets the connection to None and raises a warning.

        Raises: Warning - when connection fails (file not found, etc) sets connection to None
        """
        try: 
            self.connection = sq.connect(db_path)
            self.cursor = self.connection.cursor()
            self.write = write        
        except Exception as e: 
            raise Warning("error initializing database connection:\n" + str(e))
            self.connection = None
        
    def __del__(self):
        '''close connection before interpreter-handled removal steps'''
        self.connection.close()

    def state_check(self):
        """
        Ensures that the db connection was made and that write permission is enabled.
        Raises appropriate warnings when these conditions fail and returns None.
        If the conditions are met, returns 1 as a default 'not None' value.

        Returns: None or 1

        Raises: RuntimeWarning - when connection is None or write permission disabled.
        """
        if self.connection is None:
            raise RuntimeWarning('conneciton cursor is None: no action taken')
            return None
        if not self.write:
            raise RuntimeWarning('write permission not activated. Use self.auth() to allow writing. No changes have been made.')
            return None
        return 1
        
    def end_edit(self):
        """Turns off write authentication and commits transaction changes."""
        self.write = False
        self.connection.commit()

    # this function compares two lists to ensure the keys in a user-given dictionary
    # match the keys required for the database schema
    def verify_keys(const_tuple, check_list):
        """Verifies a set keys (checklist) match database attribute names."""
        const_list = list(const_tuple)
        return const_list == check_list
    
    # return structure: [ (well, [file, file, ...]), (well, [file, file, ...]), ...]
    # a list of 2-tuples: first entry is well info, second entry is a list
    # of all corresponding files for the well. Well AND file info are dictionaries
    # each file dictionary contains a list of boxes, stored as dictionaries.
    # this function is designed to accomodate searches that result in multiple wells
    def grid_search(self, type, value, df_out = False):
        """
        Given a value and a 'type' of search, executes a search of the database.
        Results are returned either as a nested structure or as a spreadsheet-like DataFrame.

        Parameters:
        -----------
        type:   FILE, API, OPERATOR, LEASE, or FM. searches on operator, lease, and formation
                are fuzzy searches using LIKE.
        value:  the value used in the search query. If type is FILE, the value should be file #.
        df_out: optionally return the data frame instead of the nested structure.

        Returns:
        --------
        A list of wells, each of which contains nested file and box data:
        [ (well, [file, file, ...]), (well, [file, ...]) ]

        well: a dictionary of well-related values
              {'api': , 'operator': , ...}
        file: a dictionary of file-related values
              {'file_num': , 'collection': , ..., 'boxes': [box, box, ...]}
        box:  a dictionary of box values. A list of these is a value inside each file dicitonary.
              {'file_num': , 'box_num': , 'top' , ...}
        """

        if type == "FILE":
            q_str = self.GRID_QUERY + 'file_num = ?'
        elif type == "API":
            q_str = self.GRID_QUERY + 'api = ?'
        elif type == "OPERATOR":
            q_str = self.GRID_QUERY + "operator LIKE ?"
            value = "%"+value+"%"
        elif type == "FM":
            q_str = self.GRID_QUERY + 'formation LIKE ?'
            value = "%"+value+"%"
        elif type == "LEASE":
            q_str = self.GRID_QUERY + 'lease LIKE ?'
            value = "%"+value+"%"
        else:
            raise ValueError('invalid query type: ', type)

        res = self.cursor.execute(q_str, (value,))

        #get column names from the 7-tuple res.description
        col_names = [tup[0] for tup in res.description]

        # create df, get list of unique apis, and start a list of wells
        result_df = pd.DataFrame(data = res.fetchall(), columns = col_names)
        if df_out: return result_df
        
        api_list = np.unique(result_df['api'])
        well_list = []
        
        # the following loop formats/nests the data before returning
        # rather than returning a DataFrame with redundant entries.
        for api in api_list:
            well_df = result_df[result_df['api'] == api]

            well_dict = {'api': well_df['api'].iloc[0],
                        'operator': well_df['operator'].iloc[0],
                        'well_num': well_df['well_num'].iloc[0],    
                        'lease': well_df['lease'].iloc[0],
                        'sec': well_df['sec'].iloc[0],
                        'twn': well_df['twn'].iloc[0],
                        'twn_d': well_df['twn_d'].iloc[0],
                        'rng': well_df['rng'].iloc[0],
                        'rng_d': well_df['rng_d'].iloc[0],
                        'qq': well_df['qq'].iloc[0],
                        'lat': well_df['lat'].iloc[0],
                        'long': well_df['long'].iloc[0],
                        'county': well_df['county'].iloc[0],
                        'state': well_df['state'].iloc[0],
                        'field': well_df['field'].iloc[0]}

            # get unique file_nums within well, start list of files
            filenum_list = np.unique(well_df['file_num'])
            file_list = []

            for filenum in filenum_list:
                file_df = well_df[well_df['file_num'] == filenum]

                file_dict = {'file_num': file_df['file_num'].iloc[0],
                        'collection': file_df['collection'].iloc[0],
                        'sample_type': file_df['sample_type'].iloc[0],
                        'box_count': file_df['box_count'].iloc[0],
                        'box_type': file_df['box_type'].iloc[0],
                        'diameter': file_df['diameter'].iloc[0],
                        'location': file_df['location'].iloc[0],
                        'boxes': []}

                for box_index in range(len(file_df['box_num'])):
                    box = {'box_num': file_df['box_num'].iloc[box_index],
                            'top': file_df['top'].iloc[box_index],
                            'bottom': file_df['bottom'].iloc[box_index],
                            'formation': file_df['formation'].iloc[box_index],
                            'condition': file_df['condition'].iloc[box_index],
                            'comments': file_df['comments'].iloc[box_index]}
                    file_dict['boxes'].append(box)
                
                file_list.append(file_dict)

            well_list.append((well_dict, file_list))

        if len(well_list) == 0: return None
        return well_list

    # given a file number and a dictionary of named values
    # updates values for a box. Can be used repeatedly
    # before committing changes manually.
    def modify_box(self, file_num, box_num, value_dict):
        """
        Modifies a specific box given a dictionary of attributes + values.

        Parameters:
        -----------
        file_num:   file number for the box to be modified
        box_num:    box number of box to be modified
        value_dict: dictionary of box attributes with new values

        Returns:
        --------
        A string of either the executed query (success) or an error message (failure).
        Returns None if state_check() fails.
        """
        
        if self.state_check() is None: return
        
        # BUILD QUERY
        set_values = ', '.join([f'{k} = :{k}' for k in value_dict.keys()])        
        value_dict['_file'] = file_num
        value_dict['_box'] = box_num
        
        q_str_box = f'''UPDATE Box SET {set_values}
                WHERE file_num = :_file AND box_num = :_box'''

        # EXECUTE
        try: self.cursor.execute(q_str_box, value_dict)
        except Exception as e:
            return 'error modifying Box \n' + str(e)

        return(q_str_box) # return query as string to indicate success

    # given file_num, modify file information using a dictionary of values
    # if file_num itself is changed, other tables are updated too
    def modify_file(self, file_num, value_dict):
        """
        Modifies a file given a dictionary of attributes + values.

        Parameters:
        -----------
        file_num:   file number for the file to be modified
        value_dict: dictionary of file attributes with new values

        Returns:
        --------
        A string of either the executed query (success) or an error message (failure).
        May return None if state_check() fails.
        """
        if self.state_check() is None: return

        set_values = ', '.join([f'{k} = :{k}' for k in value_dict.keys()])
        value_dict['_file'] = file_num

        q_str_file = f'UPDATE File SET {set_values} WHERE file_num = :_file'

        try: self.cursor.execute(q_str_file, value_dict)
        except Exception as e:
            return 'error modifying File\n' + str(e)

        return q_str_file

    # given an API, edit well information using a dictionary of values
    # API only needs changed in 'Well' as the DB auto-updates in other tables
    def modify_well(self, api, value_dict):
        """
        Modifies a specific box given a dictionary of attributes + values.

        Parameters:
        -----------
        api:        api number of the well to be modified
        value_dict: dictionary of well attributes with new values

        Returns:
        --------
        A string of either the executed query (success) or an error message (failure).
        Returns None if state_check() fails.
        """
        if self.state_check() is None: return

        set_values = ', '.join([f'{k} = :{k}' for k in value_dict.keys()])
        value_dict['_api'] = api

        q_str_well = f'UPDATE Well SET {set_values} WHERE api = :_api'

        try: self.cursor.execute(q_str_well, value_dict)
        except Exception as e:
            return 'error modifying api in Well table\n' + str(e)

        return q_str_well

    # Add box given an existing file number and a dictionary of values
    def add_box(self, file_num, value_dict):
        """
        Adds a new file given a dictionary of attributes with values.

        Parameters:
        -----------
        file_num:  file number to be added
        valuedict: dictionary of attributes with values. If file number 
                   is included here it is overwritten by the value of file_num above.

        Returns:
        --------
        A string of either the executed query (success) or an error message (failure).
        Returns None if state_check() fails.
        """
        if self.state_check() is None: return

        value_dict['file_num'] = file_num
        update_tup = tuple( [value_dict[k] for k in self.box_attr]  )

        try: self.cursor.execute(self.ADD_BOX, update_tup)
        except Exception as e:
            return f'error adding box to file{file_num}\n' + str(e)

        return f"added box {value_dict} to file #{file_num}"

    # Add file given an existing api/well
    def add_file(self, api, value_dict):
        """
        Adds a new well given a dictionary of attributes with values.

        Parameters:
        -----------
        valuedict: dictionary of attributes with values. If api number 
                   is included here it is overwritten by the value of api above.

        Returns: A string of either the executed query (success) or an error message (failure).
                 Returns None if state_check() fails.
        """
        if self.state_check() is None: return

        update_tup = tuple( [value_dict[k] for k in self.file_attr]  )

        # Add to well_file first to ensure file has a well/API on record
        try: self.cursor.execute('INSERT INTO well_file VALUES (?, ?)', (api, value_dict['file_num']))
        except Exception as e:
            return f'error adding file to api {api}\n' + str(e)

        try: self.cursor.execute(self.ADD_FILE, update_tup)
        except Exception as e:
            return f'error adding file to api {api}\n' + str(e)

        return f"added file: {value_dict} to api {api}"

    # Add a new well
    def add_well(self, value_dict):
        """
        Adds a new well given a dictionary of attributes with values. Must include API,
        or else database constraints fail.

        Returns: A string of either the executed query (success) or an error message (failure).
                 Returns None if state_check() fails.
        """
        if self.state_check() is None: return        

        update_tup = tuple( [value_dict[k] for k in self.well_attr] )

        try: self.cursor.execute(self.ADD_WELL, update_tup)
        except Exception as e:
            return f"error adding well: {value_dict}\n" + str(e)  

        return f"added well: {value_dict}"         

    # Remove well
    def remove_well(self, api):
        """
        Removes an existing well from the database. Well is identified by the primary key, api.

        Returns: A string confirming deletion, or an error message. Returns None if state_check() fails.  
        """
        if self.state_check() is None: return

        try: self.cursor.execute(self.DELETE_WELL, (api,))
        except Exception as e:
            return f'error removing well: {api}\n' + str(e)

        return f'deleted well: {api}'

    # Remove file
    def remove_file(self, file_num):
        """
        Removes an existing file from the database. File is identified by the primary key, file_num.

        Returns: A string confirming deletion, or an error message. Returns None if state_check() fails.  
        """
        if self.state_check() is None: return

        try: self.cursor.execute(self.DELETE_FILE, (file_num,))
        except Exception as e:
            return f'error removing file: {file_num}\n' + str(e)

        return f'deleted file: {file_num}'
        
    # Remove box
    def remove_box(self, file_num, box_num):
        """
        Removes an existing box from the database. Well is identified by the primary key, (file_num, box_num).

        Returns: A string confirming deletion, or an error message. Returns None if state_check() fails.  
        """
        if self.state_check() is None: return
    
        try: self.cursor.execute(self.DELETE_BOX, (file_num, box_num))
        except Exception as e:
            return f'error removing box: {file_num}.{box_num}'

        return f'deleted box: {file_num}.{box_num}'



    


def main():
    #db_path = '../opic_core.db'
    print(os.getcwd())
    obj = OPIC_DBC('db_api/test.db')
    obj.write = True

    '''
    
    print('\n----------TESTING modify_box(): -----------\n')

    bx_old = 1
    bx_new = 'N-23'
    f = '1X'

    # update changed values to old values
    update_dict = {'box_num' : bx_old}
    obj.modify_box(f, bx_new, update_dict)

    # display pre-update DB entry
    print(obj.cursor.execute('SELECT * FROM Box WHERE file_num = ?', (f,)).fetchall())

    update_dict = {'box_num': bx_new}
    print('\n', update_dict, '\n')
    obj.modify_box(f, bx_old, update_dict)
    
    print(obj.cursor.execute('SELECT * FROM Box WHERE file_num = ?', (f,)).fetchall())
    
    print('\n----------TESTING modify_file(): -----------\n')
    f_new = '1X'
    f_old = '1A'

    # reset old values
    update_dict = {'file_num': f_old, 'box_type': 'Slab Pack'}
    obj.modify_file(f_new, update_dict)
    obj.connection.commit()
    
    print(obj.cursor.execute('SELECT * FROM File WHERE file_num = ?;', (f_old, )).fetchall())
    print(obj.cursor.execute('SELECT * FROM well_file WHERE file_num = ?', (f_old, )).fetchall())
    print(obj.cursor.execute('SELECT * FROM Box WHERE file_num = ?', (f_old, )).fetchall())

    update_dict = {'file_num': f_new, 'box_type': 'XXL BOX'}
    print('\n', update_dict, '\n')
    obj.modify_file(f_old, update_dict)
    obj.connection.commit()
    
    print(obj.cursor.execute('SELECT * FROM File WHERE file_num = ?', (f_new, )).fetchall())
    print(obj.cursor.execute('SELECT * FROM well_file WHERE file_num = ?', (f_new, )).fetchall())
    print(obj.cursor.execute('SELECT * FROM Box WHERE file_num = ?', (f_new, )).fetchall())

    # CHECK modify_well()
    # ensure changing API maintains integrity in well_file

    print('\n----------TESTING modify_well(): -----------\n')
    api_old = 35073355020000
    api_new = 666

    op_old = 'King & Stevenson'
    op_new = 'MASON D. OPERATOR'
    
    # reset old values
    update_dict = {'api': api_old, 'operator': op_old}
    obj.modify_well(api_new, update_dict)
    obj.connection.commit()
    
    print(obj.cursor.execute('SELECT * FROM Well WHERE api = ?', (api_old,)).fetchall())
    print(obj.cursor.execute('SELECT * FROM well_file WHERE api = ?', (api_old,)).fetchall())
    
    update_dict = {'api': api_new, 'operator': op_new}
    print('\n', update_dict, '\n')
    obj.modify_well(api_old, update_dict)
    obj.connection.commit()

    print(obj.cursor.execute('SELECT * FROM Well WHERE api = ?', (api_new,)).fetchall())
    print(obj.cursor.execute('SELECT * FROM well_file WHERE api = ?', (api_new,)).fetchall())
    '''

    
    print('\n----------TESTING add_box(): -----------\n')

    file_num = 1
    box_dict = {'box_num': 333,'top': 1234,'bottom': 2345,'formation': 'Deeepwater Fm',
    'condition': 'Good','comments': 'this box contains certifiably cool stuff'}
    print(obj.cursor.execute('DELETE FROM Box WHERE file_num = ? AND box_num = ?', (file_num, box_dict['box_num'])).fetchall())
    print(obj.cursor.execute('SELECT * FROM Box WHERE file_num = ?', (file_num,)).fetchall())
    print(obj.add_box(file_num, box_dict))
    obj.connection.commit()
    print(obj.cursor.execute('SELECT * FROM Box WHERE file_num = ?', (file_num,)).fetchall())

    print('\n----------TESTING add_file(): -----------\n')

    api_num = 35073355890000
    file_dict = {'file_num': '2X', 'collection': 'OLD CORE', 'sample_type': 'butt + slab',
                'box_count': 42, 'box_type': '4 X 4 Butt Box', 'diameter': '3.3', 'location': '??'}

    # delete test values
    obj.cursor.execute('DELETE FROM File WHERE file_num = \'2X\'')

    print(obj.cursor.execute('SELECT * FROM well_file WHERE api = ?', (api_num,)).fetchall())
    print(obj.cursor.execute('SELECT * FROM File WHERE file_num = \'2X\'').fetchall())
    print(obj.add_file(api_num, file_dict))
    obj.connection.commit()
    print(obj.cursor.execute('SELECT * FROM well_file WHERE api = ?', (api_num,)).fetchall())
    print(obj.cursor.execute('SELECT * FROM File WHERE file_num = \'2X\'').fetchall())
    
    print('\n----------TESTING add_well(): -----------\n')

    new_api = 1111111111111
    obj.cursor.execute('DELETE FROM Well WHERE api = ?', (new_api,))

    well_dict = {'api': new_api, 'operator': 'Mason\'s Operating Co', 'lease': 'Hoss',
                'well_num': 95, 'sec': 36, 'twn': 2, 'twn_d': 'N', 'rng': 3, 'rng_d': 'W',
                'qq': 'NENESESE', 'lat': -20.2123435, 'long': 55.1234567, 'county': 'Camden',
                'state': 'unknown', 'field': ''}

    print(obj.add_well(well_dict))
    print(obj.cursor.execute('SELECT * FROM Well WHERE api = ?', (new_api, )).fetchall())

main()    
