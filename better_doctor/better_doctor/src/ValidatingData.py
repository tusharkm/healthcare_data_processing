import pandas as pd
from better_doctor.src.MatchingData import MatchingData
class ValidatingData:
    matching_data_object = MatchingData()
    new_doctor_data = pd.DataFrame()
    new_practice_doctor_data = pd.DataFrame()
    csv_file=matching_data_object.csv_file.copy()

    # removing npi with null values
    new_csv_file_npi_cleaned=csv_file[pd.notnull(csv_file['npi'])]

    # Scanning data from JSON having doctor and practice information
    new_practice_doctor_data = matching_data_object.extarcting_practice_json(matching_data_object.JSON_FILEPATH).copy()

    # Scanning data from JSON  having only doctors data
    new_doctor_data = matching_data_object.extarcting_doctor_json(matching_data_object.JSON_FILEPATH).copy()
    print("# of json object scanned: {}".format(new_practice_doctor_data.shape))
    print("# of CSV object scanned: {}".format(matching_data_object.csv_file.shape))

    # Creating Data Frame using CSV having columns Names and Address
    new_cvs_data_name_address=matching_data_object.create_merged_column(csv_file.copy(),"name_address",[0,1,3,4,5,6,7])

    # Creating Data Frame using CSV having columns Address
    new_cvs_data_address = matching_data_object.create_merged_column(csv_file.copy(), "address", [3, 4, 5, 6, 7])

    # Creating Data Frame using CSV having columns Name and npi
    new_cvs_data_name_npi = matching_data_object.create_merged_column(csv_file.copy(), "name_npi", [0, 1, 2])

    # Creating Data Frame using JSON having columns Name and Address
    new_json_name_address=matching_data_object.create_merged_column(new_practice_doctor_data.copy(),"name_address",[0,1,5,6,7,8,9])

    # Creating Data Frame using JSON having columns Address
    new_json_address = matching_data_object.create_merged_column(new_practice_doctor_data.copy(), "address", [5, 6, 7, 8, 9])

    # Creating Data Frame using JSON having columns Name and API
    new_json_name_npi = matching_data_object.create_merged_column(new_practice_doctor_data.copy(), "name_npi", [0,1,2])

    # b) number of Doctors matched with NPI
    matched_doctor_on_npi = pd.merge(new_doctor_data, new_csv_file_npi_cleaned, on=['npi'], how='inner')
    print('# of number of Doctors matched with NPI: {}'.format(matched_doctor_on_npi.shape[0]))

    # c) number of Practices matched with NPI
    matched_practices_on_npi = pd.merge(new_practice_doctor_data, matching_data_object.csv_file, on=['npi'], how='inner')
    print('# of Practices matched with NPI: {}'.format(matched_practices_on_npi.shape[0]))


    # of Doctors matched with name and address
    matching_data_object.matched_doctor_name_address = pd.merge( new_json_name_address,new_cvs_data_name_address, on = 'name_address',how='inner')
    print('# of Doctors matched with name and address: {}'.format(matching_data_object.matched_doctor_name_address.shape[0]))


    # of Practice matched with address
    matched_doctor_address = pd.merge(new_json_address, new_cvs_data_address, on='address', how='inner')
    print("# of Practice matched with address: {}".format(matched_doctor_address.shape[0]))

    # of practices matched with Name and NPI
    matched_doctor_name_npi = pd.merge(new_json_name_npi, new_cvs_data_name_npi, on='name_npi', how='inner')
    print("# of Practice matched with Name and NPI: {}".format(matched_doctor_name_npi.shape[0]))

    # Number of unmatched
    matched_doctor_name_npi_outer = pd.merge(new_json_name_npi, new_cvs_data_name_npi, on='name_npi', how='outer')
    null_count = len(matched_doctor_name_npi_outer) - matched_doctor_name_npi_outer.count()
    print("# of unmatched Document when matched on Name and NPI: {}".format(null_count.npi_x+null_count.npi_y))
    #print(null_count)

