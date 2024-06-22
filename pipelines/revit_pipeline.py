import os

from etls.revit_etl import authenticate, get_file_urn, extract_param_data, transform_data, load_data_to_csv
from utils.constants import BIM_360_PROJECT_ID, BIM_360_FOLDER_ID, REVIT_FILE_NAME, REVIT_MODEL_GUID, OUTPUT_PATH


def revit_pipeline(file_name):
    # Connect to Revit model
    token = authenticate()
    urn = get_file_urn(token, BIM_360_PROJECT_ID, BIM_360_FOLDER_ID, REVIT_FILE_NAME)
    
    # Data extraction
    unit_df = extract_param_data(token, urn, REVIT_MODEL_GUID)
    
    # Data transformation
    unit_trans_df = transform_data(unit_df)

    # Data loading to csv
    file_path = os.path.join(OUTPUT_PATH, f'{file_name}.csv')
    load_data_to_csv(unit_trans_df, file_path)

    return file_path