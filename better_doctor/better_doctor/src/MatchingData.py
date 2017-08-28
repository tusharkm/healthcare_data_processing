import pandas as pd
import json
import string
import re
regex = re.compile('[%s]' % re.escape(string.punctuation))
import os

class MatchingData:
    DIR = os.path.dirname(__file__)
    CSV_FILEPATH = os.path.join(DIR, '../data_files/match_file.csv')
    CSV_FILEPATH = os.path.abspath(os.path.realpath(CSV_FILEPATH))

    JSON_FILEPATH = os.path.join(DIR, '../data_files/source_data.json')
    JSON_FILEPATH = os.path.abspath(os.path.realpath(JSON_FILEPATH))


    csv_file = pd.read_csv(CSV_FILEPATH).copy()
    json_file = pd.read_json(JSON_FILEPATH, lines=True).copy()

    """Converts JSON into flatten Dataframe to extract practise information from JSON
    Input: Json file
    Output: Dataframe"""
    def extarcting_practice_json(self,file):
        columns = ['first_name', 'last_name', 'npi', 'lat', 'lon', 'street', 'street_2', 'city', 'state', 'zip']
        fname_list = []
        lname_list = []
        npi_list = []
        lat_list = []
        lon_list = []
        street_list = []
        street_2_list = []
        city_list = []
        state_list = []
        zip_list = []
        with open(file, 'r+') as infile:
            for line in infile:
                line = line.replace("\\", r"\\")
                line = json.loads(line)
                for index, elem in enumerate(line['practices']):
                    fname_list.append(line['doctor']['first_name'])
                    lname_list.append(line['doctor']['last_name'])
                    npi_list.append(line['doctor']['npi'])
                    lat_list.append(line['practices'][index]['lat'])
                    lon_list.append(line['practices'][index]['lon'])
                    street_list.append(line['practices'][index]['street'])
                    street_2_list.append(line['practices'][index]['street_2'])
                    city_list.append(line['practices'][index]['city'])
                    state_list.append(line['practices'][index]['state'])
                    zip_list.append(line['practices'][index]['zip'])
        new_json_data = pd.DataFrame(
        [fname_list, lname_list, npi_list, lat_list, lon_list, street_list, street_2_list, city_list,
        state_list,zip_list]).T
        new_json_data.columns =[columns]
        return new_json_data

    """Converts JSON into flatten Dataframe to extract doctor information from JSON
    Input: Json file
    Output: Dataframe"""

    def extarcting_doctor_json(self,file):
        columns = ['first_name', 'last_name', 'npi']
        unique_fname_list = []
        unique_lname_list = []
        unique_npi_list = []
        with open(file, 'r+') as infile:
            for line in infile:
                line = line.replace("\\", r"\\")
                line = json.loads(line)
                for index, elem in enumerate(line['practices']):
                    if line['doctor']['npi'] not in unique_npi_list:
                        unique_fname_list.append(line['doctor']['first_name'])
                        unique_lname_list.append(line['doctor']['last_name'])
                        unique_npi_list.append(line['doctor']['npi'])
        new_json_data = pd.DataFrame(
        [unique_fname_list,unique_lname_list,unique_npi_list]).T
        new_json_data.columns =[columns]
        return new_json_data

    """Cleanse the date by taking a list and by changing the data to lower case then removing
    any special character in the fields and finally  creating a single field with the spaces removed
    Input: List
    Output: normalized data"""
    def clean_lower_data(self,field):
        field=field.apply(lambda x: x.lower())
        field=field.apply(lambda x: regex.sub('', x))
        field=field.apply(lambda x: x.replace(' ', ''))
        return field


    """The below function merged the data of the column specified by the column indices given in the function.
     It handled nan values by replacing it by ‘_’. the column created can be used to perform joins.
    Input: 	a. File Name
    b. Column name to be created
    c. List of column indices
	Output: File with new column appended
"""
    def create_merged_column(self,file,column_name,columnList=[]):
        add=[]
        for i in columnList:
            address = []
            address= file.apply(lambda row: row.iloc[i] if pd.notnull(row.iloc[i]) else '_', axis=1 ) #Replaced Nan with '_'
            if len(add)==0:
                add=address
            else :
                add=add+address
        file[column_name]=self.clean_lower_data(add)
        return file

