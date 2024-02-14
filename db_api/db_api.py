import sqlite3 as sq

db_path = '../opic_core.db'



# add well

# add file (must have corresponding well and add to well_file table)
    # add file for existing well
    # add file with a well (file + well tables updated in same function)

# add box to file

# edit well (if api changed, must record in well_file)

# edit file (given file num to search and a dict of values)

# edit box (given box num to identify box and a dict of new values)



class OPIC_DBC():

    add_File = "INSERT INTO Well VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?)"
    add_Well = "INSERT INTO File VALUES (?,?,?,?,? ?,?)"
    add_Box = "INSERT INTO Box VALUES (?,?,?,?,?, ?,?)"
    add_wellfile = "INSERT INTO well_file VALUES (?, ?)"

    get_grid = "SELECT * FROM xl_grid WHERE file_num = ?"

    full_search = '''SELECT * FROM Well, well_file, File, Box WHERE
    Well.api = well_file.api AND
    well_file.file_num = File.file_num AND
    File.file_num = Box.file_num AND '''

    def __init__(self, db_path, write = False):

        self.password = ''
        self.writePermission = write        
        self.connection = sq.connect(db_path)
        self.connection.row_factory = sq.Row
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.connection.close()

    # ADD FUNCTION TO UNPACK ROWS AND GIVE 

    def grid_search(self, type, value):

        # type: File #, API, Operator, Lease
        if type == "FILE":
            q_str = self.full_search + 'File.file_num = ?'
        elif type == "API":
            q_str = self.full_search + 'Well.api = ?'
        elif type == "OPERATOR":
            q_str = self.full_search + "Well.operator LIKE ?"
        elif type == "LEASE":
            q_str = self.full_search + "Well.lease LIKE ?"
        else:
            return 'invalid query type'

        res = self.cursor.execute(q_str, (value,))

        print(res.keys())
        print(res.fetchall())  
              
        return res

def main():
    obj = OPIC_DBC('../opic_core.db')

    rslt = obj.grid_search("FILE", 1)

    for row in rslt:
        print(row)

main()    


