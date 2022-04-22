import os
import glob
import re

try:
  
  for file_path in glob.glob(os.path.join('data/', '*.txt')):
    with open(file_path,'r') as f:
      file_name = os.path.basename(file_path)
      table_name = os.path.splitext(file_name)[0]
      
      # open file and read first row
      first_row = f.readline()
      # remove \n character
      first_row = first_row.strip()
      # populate list with column headers
      header_list = first_row.split('\t')
      create_column = ''
      
      # loop through column header
      for idx, val in enumerate(header_list):
        if re.search('_numerator', val) or re.search('_rate', val) or re.search('total', val):
          if len(create_column) == 0:
            create_column = val + ' numeric(19,10) null'
          else:
            create_column = create_column + ', ' + val + ' numeric(19,10) null'
        elif re.search('flg', val) or re.search('cnt', val):
          if len(create_column) == 0:
            create_column = val + ' char(1) null'
          else:
            create_column = create_column + ', ' + val + ' char(1) null'
        elif re.search('_id', val) or re.search('_count', val) or re.search('no_of', val):
          if len(create_column) == 0:
            create_column = val + ' bigint null'
          else:
            create_column = create_column + ', ' + val + ' bigint null'
        elif re.search('dob', val):
          if len(create_column) == 0:
            create_column = val + ' date null'
          else:
            create_column = create_column + ', ' + val + ' date null'
        elif re.search('_date', val):
          if len(create_column) == 0:
            create_column = val + ' timestamp null'
          else:
            create_column = create_column + ', ' + val + ' timestamp null'
        else:
          if len(create_column) == 0:
            create_column = val + ' text null'
          else:
            create_column = create_column + ', ' + val + ' text null'
    
      # create table sql
      create_sql = 'create table if not exists ' + 'db_schema' + '.' + table_name + ' (' + create_column + ');'
      print('-- ' + table_name)
      print(create_sql)
      print('')
      
      # push file to table
      next(f)
except:
  print('Error...')