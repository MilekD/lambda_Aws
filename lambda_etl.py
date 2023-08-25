import pandas as pd
import requests
from datetime import date, datetime
import io


URL = 'https://raw.githubusercontent.com/xosasx/nigerian-local-government-areas/master/csv/lgas.csv'
lga_df=pd.read_csv(URL, on_bad_lines='skip')
lagos=lga_df[lga_df.state_name=='Lagos']
api_key='cced63ab0e2ad36c2ea56a051713542c'


def read_data(url):
    try:
        lga_df=pd.read_csv(url, on_bad_lines='skip')
        lagos_df=lga_df[lga_df.state_name=='Lagos']
        return lagos_df
    except Exception as e:
        print(f"Error reading data: {str(e)}")
        return pd.DataFrame()
              
def openweather_api(df,api):
              
    json_list=[]
    state_list=[]
    lga_list=[]
    id_list = []
    datetime_list = []
                         
              
    for _,row in df.iterrows():
        lat=row['latitude']
        lon=row['longitude']
        state = row['state_name']
        lga=row['name']
        id = f"{lga}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        url=f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}'
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data=response.json()
                json_list.append(data)
                state_list.append(state)
                lga_list.append(lga)
                id_list.append(id)
                datetime_list.append(dt)
            else: 
              print(f"Request failed for coordinates ({lat}, {lon})")
        except Exception as e:
            print(f"Error requesting weather data: {str(e)}") 
            
    weather_data = pd.json_normalize(json_list)
    weather_data['state_name'] = state_list
    weather_data['lga_name'] = lga_list
    weather_data['time'] = datetime_list
    weather_data['idi'] = id_list
    return weather_data
 
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    it takes the raw openweather API data, cleaned it and retun a new data
    Args
    df(pd.DataFrame): takes the dataframe genrated from open weather API

    Returns:
    Clean data and filter weather data
    """

    # the first cleaning is to explode the weather nested json data to get other attributes
    if 'weather' in df.columns:
        weather_data = df.explode('weather').reset_index(drop=True)
        weather_data = pd.concat([weather_data.drop(['weather'], axis=1),
                                  weather_data['weather'].apply(pd.Series)], axis=1)

    # filtered needed data out

        data = {
            "id": weather_data['idi'],
            "station_name": weather_data['name'],
            "state": weather_data['state_name'],
            "lga": weather_data['lga_name'],
            "weather_type": weather_data['main'],
            "description": weather_data['description'],
            "longitude": weather_data["coord.lon"],
            "latitude": weather_data['coord.lat'],
            "temp(F)": weather_data['main.temp'],
            "temp_min(F)": weather_data['main.temp_min'],
            "temp_max(F)": weather_data['main.temp_max'],
            "pressure(hPa)": weather_data['main.pressure'],
            "humidity(%)": weather_data['main.humidity'],
            "wind_speed(m/s)": weather_data['wind.speed'],
            "wind_deg": weather_data['wind.deg'],
            "wind_gust(m/s)": weather_data['wind.gust'],
            "cloudiness(%)": weather_data['clouds.all'],
            "sea_atmo_pressure(hPa)": weather_data['main.sea_level'],
            "grnd_atmo_pressure(hPa)": weather_data['main.grnd_level'],
            "datetime": weather_data['time']
        }

        return pd.DataFrame(data)
    else:
        return pd.DataFrame()
 
    
def upload_dataframe_to_s3(dataframe:pd.DataFrame, bucket:str, object_name:str):
    csv_buffer = io.StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    s3_client = boto3.client('s3')
    try:
        response = s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=object_name)
        return True
    
    except Exception as e:
        print(f"Error uploading data to S3: {str(e)}")
        return False
    
def lambda_handler(event, context):
    try:
        lga = read_data(URL) #load the LGA data

        # name to all the datasets
        raw_weather_data_filename = f'Lagos_LGA_raw_Weather_data_{date.today()}_{datetime.now().strftime("%H.%M.%S")}.csv'
        cleaned_weather_data_filename = f'Lagos_LGA_cleaned_Weather_data_{date.today()}_{datetime.now().strftime("%H.%M.%S")}.csv'

        # get the raw data and save it to the stagging bucket for raw data
        raw_weather_df = openweather_api(lga,API_KEY)
        upload_dataframe_to_s3(raw_weather_df, 'mil-raw-weatherdata', raw_weather_data_filename)

        # get the raw data and save it to the stagging bucket for clean data
        cleaned_weather_df = transform_data(raw_weather_df)
        upload_dataframe_to_s3(cleaned_weather_df, 'mil-clean-weatherdata', cleaned_weather_data_filename)

        response = {'statusCode': 200, 'body': "Successfully"}
    
    except Exception as e:
        print(f"Error in Lambda function: {str(e)}")
        response = {'statusCode': 500, 'body': "Error"}

    return response    


# lambda_handler()
# x=openweather_api(read_data(URL),api_key)    
# w=transform_data(x)
