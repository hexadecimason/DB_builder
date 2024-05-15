# Clean DB so each row is a box, APIs aren't missing, and file numbers are appropriate
import pandas as pd
import numpy as np
import os
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)

# load csv
# df_master = pd.read_csv('../data/DB_master.csv')
df_master = pd.read_csv('data/DB_master.csv')

# filter out no-API wells
print(df_master['API'].str.isdecimal())
print(df_master['API'].isna())
df_noAPI = df_master[df_master['API'].isna() | ~(df_master['API'].str.isdecimal())]
df_noAPI.to_csv('data/no_api.csv', index = False)
exit()
df_master.dropna(subset = 'API', inplace=True) # drop empty values
df_filtered = df_master[df_master['API'].str.isdecimal()] # drop text entries


# function to filter out bad lat/long values
def checkfloat(num):
    try:
        return float(num)
    except:
        return np.NaN

df_filtered['Latitude'] = df_filtered['Latitude'].apply(checkfloat)
df_filtered['Longitude'] = df_filtered['Longitude'].apply(checkfloat)

# VERIFY TYPES - capital prefixes (Int vs int) allow for pd.NaN values
df_filtered['API'] = df_filtered['API'].astype('Int64')
df_filtered['Sec'] = df_filtered['Sec'].astype('Int32')
df_filtered['Tw'] = df_filtered['Tw'].astype('Int32')
df_filtered['Rg'] = df_filtered['Rg'].astype('Int32')
df_filtered['Latitude'] = df_filtered['Latitude'].astype('Float64')
df_filtered['Longitude'] = df_filtered['Longitude'].astype('Float64')

# separate entries where Box and Total are both null; append after box numbers are re-organized
nullboxes_df = df_filtered[df_filtered['Box'].isna() & df_filtered['Total'].isna()]
df_filtered.dropna(subset = ['Box', 'Total'], inplace=True)

# List of files to unpack
file_list = df_filtered['File #'].unique()

# Create empty df to fill
clean_df = pd.DataFrame(columns = df_filtered.columns.values.tolist())

for file in file_list: #'file' runs through each file

    # subset for each file, set up a blank df to hold cleaned file
    sub_df = df_filtered[df_filtered['File #'] == file]
    cleanfile = pd.DataFrame(columns = sub_df.columns.values.tolist())
    boxes_added = 0 # tracks boxes in a file

    # index through rows of each row in sub_df
    # file_row represents either a box or a set of boxes
    for file_row in range(len(sub_df)):

        # set up dummy values for counting boxes
        boxes_in_row = -1
        boxes_in_file = -1

        row_top = sub_df['Top'].iloc[file_row]
        row_bottom = sub_df['Bottom'].iloc[file_row]

        # determine which of Box/Total entries are empty
        boxNull = pd.isnull(sub_df['Box'].iloc[file_row]) and pd.notnull(sub_df['Total'].iloc[file_row])
        totalNull = pd.isnull(sub_df['Total'].iloc[file_row]) and pd.notnull(sub_df['Box'].iloc[file_row])
        noNull = pd.notnull(sub_df['Box'].iloc[file_row]) and pd.notnull(sub_df['Total'].iloc[file_row])

        # determine number of boxes represented by row		
        if boxNull: boxes_in_row = sub_df['Total'].iloc[file_row] 
        elif totalNull: boxes_in_row = sub_df['Box'].iloc[file_row] 
        elif noNull: boxes_in_row = 1

        # adds all boxes represented by individual row
        for box in range(boxes_in_row):

            if box == 0: boxtop = row_top
            else: boxtop = np.NaN

            if box == boxes_in_row - 1: boxbottom = row_bottom
            else: boxbottom = np.NaN

            # zip columns 
            row_add = dict(zip(sub_df.columns.values.tolist(),
                [sub_df['File #'].iloc[file_row],
                boxes_added + 1, # 'Box'
                boxes_in_file, # 'Total' - still == -1, will be calculated after boxes are added
                sub_df['Location'].iloc[file_row],
                sub_df['API'].iloc[file_row],
                sub_df['Operator'].iloc[file_row],
                sub_df['Lease'].iloc[file_row],
                sub_df['Well #'].iloc[file_row],
                sub_df['Sec'].iloc[file_row],
                sub_df['Tw'].iloc[file_row],
                sub_df['TwD'].iloc[file_row],
                sub_df['Rg'].iloc[file_row],
                sub_df['RgD'].iloc[file_row],
                sub_df['Quarter'].iloc[file_row],
                sub_df['Latitude'].iloc[file_row],
                sub_df['Longitude'].iloc[file_row],
                sub_df['County'].iloc[file_row],
                sub_df['State'].iloc[file_row],
                sub_df['Formation'].iloc[file_row],
                sub_df['Field'].iloc[file_row],
                boxtop, #'Top'
                boxbottom, #'Bottom'
                sub_df['Type'].iloc[file_row],
                sub_df['Box Type'].iloc[file_row],
                sub_df['Condition'].iloc[file_row],
                sub_df['Diameter'].iloc[file_row],
                sub_df['Restrictions'].iloc[file_row],
                sub_df['Comments'].iloc[file_row] ]))

            # add row to clean file
            cleanfile = pd.concat([cleanfile, pd.DataFrame([row_add])], ignore_index = True)

            boxes_added += 1

    # determine total boxes in file
    boxes_in_file = len(cleanfile)
    cleanfile = cleanfile.assign(Total = boxes_in_file)

    # add cleanfile to the aggreagate cleaned Dataframe
    print("cleaned: file", cleanfile['File #'].iloc[0])
    clean_df = pd.concat([clean_df, cleanfile], ignore_index= True)

# VERIFY TYPES
clean_df = pd.concat([clean_df, nullboxes_df], ignore_index = True)
clean_df['Box'] = clean_df['Box'].astype('Int32')
clean_df['Total'] = clean_df['Total'].astype('Int32')

# SAVE TO FILE
print("saving CSV file...")
clean_df.to_csv('/data/' + 'cleaned.csv', index = False)

