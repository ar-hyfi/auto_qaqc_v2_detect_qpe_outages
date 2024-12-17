import os
from TagEngine.TagEngine_AWS import TagEngine_AWS
from influxdb_client import InfluxDBClient
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from influxdb_client.client.warnings import MissingPivotFunction
import warnings 
warnings.simplefilter("ignore", MissingPivotFunction)

token_slack = os.environ['SLACK_TOKEN']
channel = os.environ['SLACK_CHANNEL_ID']
# channel = 'C02B4KZ2V9Z' # test channel
influx_token_prod_read = os.environ['INFLUX_TOKEN_PROD_READ']

# grabs the site names and codes of all sites that are marked "installed" or "maintenance required"
def get_sites():
    assets = TagEngine_AWS()
    all_sites, response_list = assets.get_all_folder_tasks(16867760)

    site_dict = {}

    for site in all_sites:
        # Check if the site status is either 'installed' or 'maintenance required'
        if site['status']['status'] in ['installed', 'maintenance required']:
            site_code = None
            # Find the site code in custom fields
            for field in site['custom_fields']:
                if field['id'] == '9ddd3d21-ab47-4f6c-966d-00854091501e':
                    site_code = field.get('value', 'Unknown Site Code')
                    break
            if site_code:
                # Map the site code to the site name
                site_dict[site_code] = site['name']

    return site_dict

# reads data from InfluxDB and formats it into dataframe
def read_from_influx(query, token):
    try:
        with InfluxDBClient(url="https://us-west-2-1.aws.cloud2.influxdata.com", 
                            token=token, 
                            org="hyfi") as read_client:
            data_frames = read_client.query_api().query_data_frame(org="hyfi", query=query)

        if data_frames is None or (isinstance(data_frames, list) and not data_frames):
            print("No data returned")
            return pd.DataFrame()

        # Concatenate if it's a list of DataFrames, else use the single DataFrame
        df = pd.concat(data_frames, ignore_index=True) if isinstance(data_frames, list) else data_frames

        # Convert '_time' column to datetime if it exists and is not in datetime format
        if '_time' in df.columns:
            df['_time'] = pd.to_datetime(df['_time'])

        # Sort the DataFrame by '_time' in ascending order
        df = df.sort_values(by='_time', ascending=True)

        return df  # This line should be inside the try block

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

def send_slack_message(token, channel, message, file_path=None):
    client = WebClient(token=token)
    try:
        if file_path:
            # Upload the file to Slack using the newer, more stable method
            response = client.files_upload_v2(channels=channel, file=file_path, initial_comment=message)
        else:
            # Send a message to Slack
            response = client.chat_postMessage(channel=channel, text=message)
    except SlackApiError as e:
        print(f"Slack API Error: {e.response['error']}")

all_sites = get_sites()

no_qpe01h = []
for site_code in all_sites.keys():
    query = f'''
        from(bucket: "prod")
        |> range(start: -3h)
        |> filter(fn: (r) => r.site_code == "{site_code}")
        |> filter(fn: (r) => r._measurement == "qpe_01h")
        '''
    # print('Query:', query)

    df = read_from_influx(query, token=influx_token_prod_read)

    # check if the dataframe returns error
    if 'error' in df.columns and df['error'].eq(500).any():
        message = '500 error. Check logs. Might be an issue with InfluxDB.'
        send_slack_message(token_slack, channel, message)
    if df.empty:
        no_qpe01h.append(site_code)
        
print('No qpe_01h data for the following sites:', no_qpe01h)

# Set threshold for percentage of sites to alert for
outage_alert_level_1 = 0.1 # 10% of sites
outage_alert_level_2 = 0.15 # 105% of sites
outage_alert_level_3 = 0.25
outage_alert_level_4 = 0.5

total_num_sites = len(all_sites)
total_num_no_qpe_01h = len(no_qpe01h)

percentage = total_num_no_qpe_01h/total_num_sites

# Send a Slack message if the percentage of sites with no qpe_01h is above the threshold.
if percentage > outage_alert_level_1:
    message = f"{percentage:.0%} of sites have no qpe_01h data for the last 3 hours. Check Influx and AWS."
    print('Sending Slack message:', message)
    send_slack_message(token_slack, channel, message)

print('No qpe_01h outage detected.')