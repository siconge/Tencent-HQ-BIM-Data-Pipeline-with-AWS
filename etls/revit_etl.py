import requests
import numpy as np
import pandas as pd

from utils.constants import APS_CLIENT_ID, APS_CLIENT_SECRET, APS_API_BASE_URL


# Authenticate with APS (Autodesk Platform Services) API
def authenticate():
    url = f'{APS_API_BASE_URL}/authentication/v2/authenticate'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'client_id': APS_CLIENT_ID,
        'client_secret': APS_CLIENT_SECRET,
        'grant_type': 'client_credentials',
        'scope': 'data:read data:write data:create bucket:read'
    }
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()['access_token']


# Get URN of a Revit file in BIM 360
def get_file_urn(token, project_id, folder_id, file_name):
    url = f'{APS_API_BASE_URL}/data/v1/projects/{project_id}/folders/{folder_id}/search'

    # Include in the subsequent requests the access token obtained during the authentication process
    headers = {'Authorization': f'Bearer {token}'}
    params = {'q': file_name}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    files = response.json()['results']
    if len(files) == 0:
        raise ValueError(f'File "{file_name}" not found')

    # Get the first result as the desired file
    return files[0]['id']


# Extract object metadata (properties) from a Revit file, filter Revit curtain wall panel objects by family name, and store specified parameters
def extract_param_data(token, urn, model_guid):
    # Model Derivative API
    url = f'{APS_API_BASE_URL}/modelderivative/v2/designdata/{urn}/metadata/{model_guid}/properties'
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    object_data = response.json()

    # Filter curtain wall panels by family name containing "sawtooth" or "flat"
    filtered_panels = [object for object in object_data['data']['collections'] if 
                       object.get('name', '').lower() in ['sawtooth', 'flat']]
    
    # Define object parameters to extract
    params_to_extract = ['unit_id', 'bldg_no', 'level', 'unit_type', 'unit_height_m', 'unit_span_m', 'alum_panel_width_m',
                         'alum_panel_area_sm', 'glazing_width_m', 'glazing_area_sm', 'ventilation_louver', 'rescue_window']
    
    # Initialize a list to store parameter data
    unit_ls = []
    
    # Extract parameter data for each filtered panel
    for panel in filtered_panels:
        unit_params = {}
        if 'properties' in panel:
            for param_name, param_value in panel['properties'].items():
                if param_name in params_to_extract:
                    unit_params[param_name] = param_value
        unit_ls.append(unit_params)
    
    unit_df = pd.DataFrame(unit_ls)
    return unit_df


def transform_data(df:pd.DataFrame):
    # Add new columns with additional valuable information such as energy consumption, ventilation area, cost, etc. based on the original data
    df['facade_area_sm'] = df['alum_panel_area_sm'] + df['glazing_area_sm']
    df['operable_area_sm'] = np.where((df['unit_type'] == 'sawtooth') & (df['alum_panel_width_m'] >= 0.4),
                                   (df['alum_panel_width_m'] - 0.15) * (df['unit_height_m'] - 1.5), 0)
    df['unit_cost_usd'] = np.where((df['unit_type'] == 'sawtooth'), df['facade_area_sm'] * 525, df['facade_area_sm'] * 433)
    df['embodied_carbon_kgCO2e'] = (
        df['alum_panel_area_sm'] * 167 + df['glazing_area_sm'] * 95 + (df['glazing_width_m'] * 3 + df['unit_height_m'] * 2) * 59)
    df = df.round({
        'facade_area_sm':2,
        'operable_area_sm':2,
        'unit_cost_usd':2,
        'embodied_carbon_kgCO2e':2
    })

    # Convert data types
    df['ventilation_louver'] = df['ventilation_louver'].astype(bool)
    df['rescue_window'] = df['rescue_window'].astype(bool)

    # Reorder columns
    cols = df.columns.tolist()
    cols_to_shift = ['ventilation_louver', 'rescue_window']
    cols_to_keep = [col for col in cols if col not in cols_to_shift]
    new_order = cols_to_keep + cols_to_shift
    df = df[new_order]

    return df


def load_data_to_csv(data:pd.DataFrame, path:str):
    data.to_csv(path, index=0)
    print('Data extracted and saved successfully')