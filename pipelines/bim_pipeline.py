from etls.aws_etl import create_aws_session
from etls.bim_etl import get_client_credentials, authenticate, get_file_urn, get_model_guid, extract_param_data, transform_data, generate_file_path, load_data_to_csv
from mocks.mockup import test_pipeline
from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME, APS_CLIENT_CREDENTIALS_SECRET_NAME, BIM_360_PROJECT_ID, BIM_360_ITEM_ID, MODEL_VIEW_NAME, OUTPUT_PATH, OUTPUT_FILENAME_PREFIX


def bim_pipeline() -> str:
    """
    Extracts, transforms, and loads BIM data from APS into a local CSV file for subsequent processing by the AWS pipeline.
    """
    # Step 1: Authenticate with APS and retrieve token
    session = create_aws_session(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME)
    client_credentials = get_client_credentials(session, APS_CLIENT_CREDENTIALS_SECRET_NAME)
    token = authenticate(*client_credentials)

    # Step 2: Retrieve URN of a specified Revit file and modelGuid of a specific view in the file
    urn = get_file_urn(token, BIM_360_PROJECT_ID, BIM_360_ITEM_ID)
    model_guid = get_model_guid(token, urn, MODEL_VIEW_NAME)
    
    # Step 3: Extract, transform, and save data
    unit_df = extract_param_data(token, urn, model_guid)
    unit_trans_df = transform_data(unit_df)
    file_path = generate_file_path(OUTPUT_PATH, OUTPUT_FILENAME_PREFIX)
    load_data_to_csv(unit_trans_df, file_path)

    return file_path
