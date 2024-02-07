import pandas as pd
import numpy as np
import sqlite3 as sq

db_path = '../opic_core.db'
clean_df = pd.read_csv('../data/cleaned.csv')
collection_name = 'OLD CORE'

# basic insert quieries for each table
q_addwell = "INSERT INTO Well VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
q_addfile = "INSERT INTO File VALUES (?, ?, ?, ?, ?, ?, ?)"
q_addbox = "INSERT INTO Box VALUES (?, ?, ?, ?, ?, ?, ?)"
q_wellfile = "INSERT INTO well_file VALUES (?, ?)"

# DB connection
con = sq.connect('../opic_core.db')
curs = con.cursor()

# empty DB before inserting new values
curs.execute('DELETE FROM well_file')
curs.execute('DELETE FROM File')
curs.execute('DELETE FROM Well')
curs.execute('DELETE FROM Box')

# loop through each unique API
for api in clean_df['API'].unique():

    # subset well from whole data frame
    well_df = clean_df[clean_df['API'] == api]
    
    # create well structure
    well = {'api' : int(well_df['API'].iloc[0]), # wrong value inserted if API is not manually cast
            'operator' : well_df['Operator'].iloc[0],
            'lease' : well_df['Lease'].iloc[0],
            'well_num' : well_df['Well #'].iloc[0],
            'sec' : well_df['Sec'].iloc[0],
            'twn' : well_df['Tw'].iloc[0],
            'twn_d' : well_df['TwD'].iloc[0],
            'rng' : well_df['Rg'].iloc[0],
            'rng_d' : well_df['RgD'].iloc[0],
            'qq' : well_df['Quarter'].iloc[0],
            'lat' : well_df['Latitude'].iloc[0],
            'long' : well_df['Longitude'].iloc[0],
            'county' : well_df['County'].iloc[0],
            'state' : well_df['State'].iloc[0],
            'field' : well_df['Field'].iloc[0] }

    # ADD WELL
    curs.execute(q_addwell, (tuple(well.values())))

    # loop through each file within a well
    for file in well_df['File #'].unique():

        # subset opic file from well
        file_df = well_df[well_df['File #']  == file]

        file_dict = {'file_num' : file_df['File #'].iloc[0],
                'collection' : collection_name,
                'sample_type' : file_df['Type'].iloc[0],
                'boxes' : file_df['Total'].iloc[0],
                'box_type' : file_df['Box Type'].iloc[0],
                'diameter' : file_df['Diameter'].iloc[0],
                'location' : file_df['Location'].iloc[0] }

        # ADD FILE
        # source file may still contain errors in File # values
        # try/catch to flag these all at once
        try:
            curs.execute(q_addfile, tuple(file_dict.values()) )
            curs.execute(q_wellfile, (well['api'], file_dict['file_num']) )

            blank_idx = 1

            # loop through each line for each file
            for entry in range(len(file_df['Box'])):

                # determine box numbers
                # will be an integer, or N-1, N-2, ... for un-numbered boxes
                if np.isnan(file_df['Box'].iloc[entry]):
                    box_num = 'N-' + str(blank_idx)
                    blank_idx += 1
                else:
                    box_num = int(file_df['Box'].iloc[entry])

                bx = {'file_num' : file,
                        'box_num' : box_num,
                        'top' : file_df['Top'].iloc[entry],
                        'bottom' : file_df['Bottom'].iloc[entry],
                        'formation' : file_df['Formation'].iloc[entry],
                        'condition' : file_df['Condition'].iloc[entry],
                        'comments' : file_df['Comments'].iloc[entry]}

                # ADD BOX
                curs.execute(q_addbox, tuple(bx.values()) )
        except:
            print('error at file:', file, ' | box total: ', file_dict['boxes'])

    # each commit to DB occurs after each well and all files/boxes are added
    # print('committing: ', api)
    con.commit()
