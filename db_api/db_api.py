import sqlite3 as sq
import numpy as np
import pandas as pd

class OPIC_DBC():

    well_attr = ('api', 'operator', 'lease', 'well_num', 'sec', 'twn', 'twn_d', 'rng', 'rng_d',
                'qq', 'lat', 'long', 'county', 'state', 'field')
    file_attr = ('file_num', 'collection', 'sample_type', 'box_count', 'box_type', 'diameter', 'location')
    box_attr = ('file_num', 'box_num', 'top', 'bottom', 'formation', 'condition', 'comments')
    well_file_attr = ('api', 'file_num')
    
    add_Well = "INSERT INTO Well VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    add_File = "INSERT INTO File VALUES (?,?,?,?,?, ?,?)"
    add_Box = "INSERT INTO Box VALUES (?,?,?,?,?, ?,?)"
    add_wellfile = "INSERT INTO well_file VALUES (?, ?)"

    grid_query  = '''SELECT * FROM xl_grid WHERE '''

    def __init__(self, db_path, write = False):

        self.writePermission = write        
        self.connection = sq.connect(db_path)
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.connection.close()

    def auth(self, permission = True):
        self.writePermission = permission

    def end_edit(self):
        self.writePermission = False
        self.connection.commit()

    # this function compares two lists to ensure the keys in a user-given dictionary
    # match the keys required for the database schema
    def verify_keys(const_tuple, check_list):
        const_list = list(const_tuple)
        return const_list == check_list
    
    # return structure: [ (well, [file, file, ...]), (well, [file, file, ...]), ...]
    # a list of 2-tuples: first entry is well info, second entry is a list
    # of all corresponding files for the well. Well AND file info are dictionaries
    # each file dictionary contains a list of boxes, stored as dictionaries.
    # this function is designed to accomodate searches that result in multiple wells
    def grid_search(self, type, value):

        # type: File #, API, Operator, Lease
        # python 3.8 has no match/switch statement
        if type == "FILE":
            q_str = self.grid_query + 'file_num = ?'
        elif type == "API":
            q_str = self.grid_query + 'api = ?'
        elif type == "OPERATOR":
            q_str = self.grid_query + "operator LIKE ?"
            value = "%"+value+"%"
        elif type == "FM":
            q_str = self.grid_query + 'formation LIKE ?'
            value = "%"+value+"%"
        elif type == "LEASE":
            q_str = self.grid_query + 'lease LIKE ?'
            value = "%"+value+"%"
        else:
            return 'invalid query type'

        res = self.cursor.execute(q_str, (value,))

        #get column names from the 7-tuple res.description
        col_names = [tup[0] for tup in res.description]

        # create df, get list of unique apis, and start a list of wells
        result_df = pd.DataFrame(data = res.fetchall(), columns = col_names)
        api_list = np.unique(result_df['api'])
        well_list = []

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

            # get unique file_nums within well, start lsit of files
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

        return well_list

    # given a file number and a dictionary of named values
    # updates values for a box. Can be used repeatedly
    # before committing changes manually.
    def modify_box(self, file_num, box_num, value_dict):
        if not self.writePermission:
            return 'write permission denied. activate edit mode.'

        set_values = ', '.join([f'{k} = :{k}' for k in value_dict.keys()])        
        value_dict['_file'] = file_num
        value_dict['_box'] = box_num
        
        q_str_box = f'''UPDATE Box SET {set_values}
                WHERE file_num = :_file AND box_num = :_box'''

        try: self.cursor.execute(q_str_box, value_dict)
        except Exception as e:
            return 'error modifying Box \n' + str(e)

        return(q_str_box)

    # given file_num, modify file information using a dictionary of values
    # if file_num itself is changed, other tables are updated too
    def modify_file(self, file_num, value_dict):
        if not self.writePermission:
            return 'write permission denied. activate edit mode.'

        set_values = ', '.join([f'{k} = :{k}' for k in value_dict.keys()])
        value_dict['_file'] = file_num

        q_str_file = f'UPDATE File SET {set_values} WHERE file_num = :_file'

        # attempt to update file
        try: self.cursor.execute(q_str_file, value_dict)
        except Exception as e:
            return 'error modifying File\n' + str(e)

        return q_str_file

    # given an API, edit well information using a dictionary of values
    # API only needs changed in 'Well' as the DB auto-updates in other tables
    def modify_well(self, api, value_dict):
        if not self.writePermission:
            return 'write permission denied. activate edit mode.'

        set_values = ', '.join([f'{k} = :{k}' for k in value_dict.keys()])
        value_dict['_api'] = api

        q_str_well = f'UPDATE Well SET {set_values} WHERE api = :_api'

        # attempt update
        try: self.cursor.execute(q_str_well, value_dict)
        except Exception as e:
            return 'error modifying api in Well table\n' + str(e)

        return q_str_well

    # Add box given an existing file number and a dictionary of values
    def add_box(self, file_num, value_dict):
        if not self.writePermission:
            return 'write permission denied. activate edit mode.'

        value_dict['file_num'] = file_num
        update_tup = tuple( [value_dict[k] for k in self.box_attr]  )

        try: self.cursor.execute(self.add_Box, update_tup)
        except Exception as e:
            return f'error adding box to file{file_num}\n' + str(e)

        return f"added box {value_dict['box_num']} to file #{file_num}"

    # Add file given an existing api/well
    def add_file(self, api, value_dict):
        if not self.writePermission:
            return 'write permission denied. activate edit mode.'

        update_tup = tuple( [value_dict[k] for k in self.file_attr]  )

        try:
            self.cursor.execute(self.add_File, update_tup)
        except Exception as e:
            return f'error adding file to api {api}\n' + str(e)

        try: self.cursor.execute(self.add_wellfile, (api, value_dict['file_num']))
        except:
            return f"error adding entry to well_file: {api}: file {value_dict['file_num']}"    

        return f"added file {value_dict['file_num']} to api {api}"

    # Add a new well
    def add_well(self, value_dict):
        if not self.writePermission:
            return 'write permission denied. activate edit mode.'
        
        update_tup = tuple( [value_dict[k] for k in self.well_attr] )

        try: self.cursor.execute(self.add_Well, update_tup)
        except Exception as e:
            return f"error adding well: {value_dict}\n" + str(e)  

        return f"added well {value_dict['api']}"         

        



def main():
    #db_path = '../opic_core.db'
    obj = OPIC_DBC('api_testing/test.db')
    obj.auth()

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
    obj.cursor.execute('DELETE FROM well_file WHERE file_num = \'2X\'')

    print(obj.cursor.execute('SELECT * FROM well_file WHERE api = ?', (api_num,)).fetchall())
    print(obj.add_file(api_num, file_dict))
    obj.connection.commit()
    print(obj.cursor.execute('SELECT * FROM well_file WHERE api = ?', (api_num,)).fetchall())

    
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
