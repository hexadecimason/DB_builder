import pandas as pd

# If Excel formats a well number as a date, this can reverse it
# date -> well number (x-y)
def parseWellNum(value):
    wellNum = ''
    terms = str(value).split('-')

    if len(terms) != 2: return value

    months = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6,
    'Jul':7, 'Aug':6, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}

    if terms[0] in months:
            wellNum = str(months[terms[0]]) + '-' + str(terms[1])
        elif terms[1] in months:
            wellNum = str(months[terms[1]]) + '-' + str(terms[0])

        return wellNum

# insert escape characters to comments where " or ' exist
def parseComment(com):
	s = str(com)
	s = s.replace('"', '\\\"').replace("'", "\\\'")
	return s

def correctAPIs(df):
    backup_df = = pd.read_csv('data/XLS_backup.csv')

    for file in df['File #'].unique():
		api = backup_df[backup_df['File #'] == file]['API'].iloc[0]
		df.loc[df_master['File #'] == file, 'API'] = api

    return df

def produceNullboxes(df):
    nullboxes_df = df[df['Box'].isna() & df['Total'].isna()]
	nullboxes_df.to_csv('../data/nullboxes.csv', index = False)
	print('saved null box data')
    

def fixFile(well_num = False, comments = False, resetAPIs=False, nullboxes=False):
    master_df = pd.read_csv("../data/DB_master.csv")

    if well_num: 
        print('fixing well number formatting...')
    	df_master['Well #'] = df_master['Well #'].apply(parseWellNum)
    if comments:
        print("fixing esc characters in comments")
        df_master['Comments'] = df_master['Comments'].apply(parseComment)
    if resetAPIs:
        print("correcting APIs")
        df_master = correctAPIs(df_master)
    if nullboxes:
        produceNullboxes(df_master)
    df_master.tocsv("../data/DB_master_corrected.csv")
