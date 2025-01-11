import os
import json
import requests
import numpy as np
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from utils.constants import APS_API_BASE_URL


def get_client_credentials(secret_name:str) -> tuple:
    """
    Retrieves APS client credentials from AWS Secrets Manager.
    """
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        return None, None
    secret = json.loads(response['SecretString'])
    return secret['AWS_ACCESS_KEY_ID'], secret['AWS_SECRET_ACCESS_KEY']


def authenticate(client_id:str, client_secret:str) -> str:
    """
    Authenticates with Autodesk Platform Services (APS) API to obtain an access token.
    """
    url = f'{APS_API_BASE_URL}/authentication/v2/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json().get('access_token')


def get_file_urn(token:str, project_id:str, item_id:str) -> str:
    """
    Retrieves the URN of a Revit file in a BIM 360 project using its item ID.
    """
    url = f'{APS_API_BASE_URL}/data/v1/projects/b.{project_id}/items/{item_id}'

    # Include in the subsequent requests the access token obtained during the authentication process
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    # Extract URN from the item data
    file_data = response.json().get('data')
    if not file_data:
        raise ValueError(f'File with item_id "{item_id}" not found')
    urn = file_data.get('id')
    return urn


def get_model_guid(token:str, urn:str, model_view_name:str) -> str:
    """
    Retrieves the modelGuid of a specific view in the Revit file.
    """
    url = f'{APS_API_BASE_URL}/modelderivative/v2/designdata/{urn}/metadata'
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    model_views = response.json()['data']['metadata']
    model_guid = next((view['guid'] for view in model_views if view['name'] == model_view_name), None)
    if not model_guid:
        raise ValueError(f'Model view "{model_view_name}" not found.')
    return model_guid


def extract_param_data(token:str, urn:str, model_guid:str) -> pd.DataFrame:
    """
    Extracts object metadata (properties) from a Revit file, transforms and stores specified parameters.
    """
    url = f'{APS_API_BASE_URL}/modelderivative/v2/designdata/{urn}/metadata/{model_guid}/properties'
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # Filter curtain wall panels by family name
    objects = response.json()['data']['collections']
    filtered_panels = [obj for obj in objects if obj.get('name', '').lower() in ['sawtooth', 'flat']]
    
    # Extract specified parameters
    params_to_extract = [
        'unit_id', 'bldg_no', 'level', 'unit_type', 'unit_height_m', 'unit_span_m', 'alum_panel_width_m', 'alum_panel_area_sm', 'glazing_width_m', 'glazing_area_sm', 'ventilation_louver', 'rescue_window'
    ]
    unit_ls = []
    
    # Extract parameter data for each filtered panel
    for panel in filtered_panels:
        unit_params = {param: panel['properties'].get(param) for param in params_to_extract if 'properties' in panel}
        unit_ls.append(unit_params)    
    
    if unit_ls:
        unit_df = pd.DataFrame(unit_ls)
    else:
        raise ValueError("No valid curtain wall panels found")
    return unit_df


def transform_data(df:pd.DataFrame) -> pd.DataFrame:
    """
    Performs transformations on extracted BIM data, adds calculated columns, validates schema, and handles missing/unexpected fields.
    """
    df['facade_area_sm'] = df['alum_panel_area_sm'] + df['glazing_area_sm']
    df['operable_area_sm'] = np.where(
        (df['unit_type'] == 'sawtooth') & (df['alum_panel_width_m'] >= 0.4),
        (df['alum_panel_width_m'] - 0.15) * (df['unit_height_m'] - 1.5),
        0
    )
    df['unit_cost_usd'] = np.where(
        (df['unit_type'] == 'sawtooth'),
        df['facade_area_sm'] * 525,
        df['facade_area_sm'] * 433
    )
    df['embodied_carbon_kgCO2e'] = (
        df['alum_panel_area_sm'] * 167 + df['glazing_area_sm'] * 95 + (df['glazing_width_m'] * 3 + df['unit_height_m'] * 2) * 59
    )
    df = df.round({
        'facade_area_sm':2,
        'operable_area_sm':2,
        'unit_cost_usd':2,
        'embodied_carbon_kgCO2e':2
    })

    df['ventilation_louver'] = df['ventilation_louver'].astype(bool)
    df['rescue_window'] = df['rescue_window'].astype(bool)

    # Reorder columns
    cols = df.columns.tolist()
    cols_to_shift = ['ventilation_louver', 'rescue_window']
    cols_to_keep = [col for col in cols if col not in cols_to_shift]
    new_order = cols_to_keep + cols_to_shift
    df = df[new_order]

    # Validate for missing or unexpected columns
    required_cols = [
        'unit_id', 'bldg_no', 'level', 'unit_type', 'unit_height_m', 'unit_span_m', 'alum_panel_width_m', 'alum_panel_area_sm','glazing_width_m', 'glazing_area_sm', 'facade_area_sm', 'operable_area_sm', 'embodied_carbon_kgCO2e', 'unit_cost_usd','ventilation_louver', 'rescue_window'
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    unexpected_cols = [col for col in df.columns if col not in required_cols]
    if missing_cols or unexpected_cols:
        raise ValueError(
            f"Warning: Missing columns detected: {missing_cols}"
            f"Warning: Unexpected columns detected: {unexpected_cols}"
        )

    return df


def generate_file_path(folder_path, prefix, date_format='%Y%m%d', extension='csv') -> str:
    """
    Generates a file path following the file naming protocol.
    """
    # Ensure folder exists before generating file path
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    date_str = datetime.now().strftime(date_format)
    existing_files = [f for f in os.listdir(folder_path) if f.startswith(f'{prefix}_{date_str}')]
    
    # Incrementally number suffix for file names with the same date
    suffix = str(len(existing_files) + 1).zfill(2)
    
    file_path = os.path.join(folder_path, f'{prefix}_{date_str}_{suffix}.{extension}')
    return file_path


def load_data_to_csv(data:pd.DataFrame, file_path:str) -> None:
    """
    Writes transformed data to CSV.
    """
    try:
        data.to_csv(file_path, index=False)
        print(f"Data extracted and saved successfully at {file_path}")
    
    # Raise errors to propagate failures to Airflow
    except Exception as e:
        raise RuntimeError(f"Error saving data to {file_path}: {e}")
