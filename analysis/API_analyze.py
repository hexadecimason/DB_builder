# This script is designed to identify missing or incomplete information in the existing OPIC spreadsheet-based database
# Given the missing information, the database can more effectively be parsed into a PgSQL database

import pandas as pd
import numpy as np
from text_histogram import histogram
import os.path
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)

# load CSV into a DF
master_csv = pd.read_csv('../data/DB_master.csv', low_memory = False)
df_master = pd.DataFrame(master_csv)

# Select needed columns: many are not needed for this purpose
df_clean = df_master[['File #', 'Box', 'Total', 'API', 'Operator', 
                    'Lease', 'Well #', 'Top', 'Bottom', 'Type', 'Comments']]

##################### PROCESS APIs ##########################
# FLAG: wells with missing API
# [0] = array of file #s
# [1] = array of APIs/API descriptors where no API exists
bad_api = [[], []]
null_boxes = [] # lines with API, but no box number and no box total
valid_apis = 0

# Extract file #s with bad API entries
for file in df_clean['File #'].unique():
	sub_df = df_clean[df_clean['File #'] == file]

	# find bad apis and their corresponding file
	if not (sub_df['API'].astype(str).iloc[0].isdecimal()):
		bad_api[0].append(sub_df['File #'].iloc[0])
		bad_api[1].append(sub_df['API'].iloc[0])
	# filter out good api files with null box entries
	else:
		valid_apis += 1
		for i in range(len(sub_df['Box'])):
			if pd.isnull(sub_df['Box'].iloc[i]) and pd.isnull(sub_df['Total'].iloc[i]):
				null_boxes.append(file)

################# OUTPUT for APIs #################

# DF for all bad APIs
badapi_dict = {'file' : bad_api[0], 'api entry' : bad_api[1]}
bad_api_df = pd.DataFrame(data = badapi_dict)

# Grouped DF
# replace "changed to..." values for better grouping
# '#' prefix to indicate entry values not originally present
bad_api_df['api entry'].replace(to_replace = ".hanged.*", value = "## Reassigned",
							regex = True, inplace = True)
bad_api_df['api entry'].replace(to_replace = ".*(Combined|.dded).*", value = "## Combined into other file",
							regex = True, inplace = True)
bad_api_df['api entry'].replace(to_replace = ".*isposal.*", value = "## disposal: no API",
							regex = True, inplace = True)
bad_api_df['api entry'].fillna('## empty field/NaN', inplace=True)

print("\n############### API summary: ##################################\n")
print("valid APIs: ", valid_apis)
print("number of entries: ", len(df_clean["File #"].unique()))
print("\n############### invalid API summary: #########################\n")
print(bad_api_df['api entry'].value_counts())
print("\nnumber of invalid APIs: ", sum(bad_api_df['api entry'].value_counts()))

################# Idenify wells with APIs but nulled box + totals

nullseries = pd.Series(null_boxes)
null_files = nullseries.unique()
null_count = nullseries.value_counts(sort = False)

print("\n############### good APIs, NULL box numbers: ######################")
print("\nTotal: ", len(nullseries), "entries in ", len(null_files), " files")

boxcounts = []
for num in null_files:
	sub_df = df_clean[df_clean['File #']==num]
	boxcounts.append(len(sub_df['File #']))

null_df = pd.DataFrame({"file #":null_files, 
                       "null entries":null_count,
                       "total boxes": boxcounts,
                       "% null": np.round(null_count*100/boxcounts)})

print("\n\t\thistogram of % null boxes per file:\n")
histogram(null_df['% null'], minimum=0, maximum=100, custbuckets="0,25,50,75,99.9,100", hscale=2)


print("\n\t\thistogram of total number null boxes per file:\n")
histogram(null_df['null entries'], minimum=0, maximum=20, custbuckets="1,2,3,4,5,10,20",hscale=2)

################# examine nullboxes.csv 

if os.path.isfile('data/nullboxes.csv'):
	boxless_csv = pd.read_csv('data/nullboxes.csv', low_memory = False)
	null_df = pd.DataFrame(boxless_csv)
	print("\nDB entries with no box numbers,  by sample type: ")
	print(null_df['Type'].value_counts())
