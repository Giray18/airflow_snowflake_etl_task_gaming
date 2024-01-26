from faker import Faker
from faker.providers import DynamicProvider

def create_dataframe(table_name,size=1000):
    '''
    By this function a fake value holding table being created to replicate a table
    that reflects given table structures by task documents.
    '''
    import pandas as pd
    fake = Faker()
                
    #Unique ID dataset provider
    UID_provider = DynamicProvider(
    provider_name="UID_provider",
    elements=[*range(1, 1100, 1)]
    )

    fake.add_provider(UID_provider)
    
    #Random ID dataset provider
    ID_provider = DynamicProvider(
        provider_name="ID_provider",
        elements=[*range(0, 10, 1)]
    )

    fake.add_provider(ID_provider)

    
    
    #ship_name dataset provider
    ship_name_provider = DynamicProvider(
        provider_name="ship_name_provider",
        elements=['frigates','frigates_1','frigates_2','frigates_3','frigates_4']
    )

    fake.add_provider(ship_name_provider)
    
    # Apeend values into a dict by conditions over column names or data types
    col_dataframe_names = {}

    for col_name,data_type in table_name.items():
        if "TIMESTAMP" in data_type:
            col_dataframe_names[col_name] = [fake.date_this_month() for time_stamp in range(size)]
        elif "SESSION_ID" in col_name:
            col_dataframe_names[col_name] = [fake.unique.UID_provider() for ID in range(size)]
        elif "USER_ID" in col_name:
            col_dataframe_names[col_name] = [fake.ID_provider() for ID in range(size)]
        elif "SHIP_NAME" in col_name:
            col_dataframe_names[col_name] = [fake.ship_name_provider() for ID in range(size)]
        elif "BOOLEAN" in data_type or "IS" in col_name:
            col_dataframe_names[col_name] = [fake.boolean() for ID in range(size)]
        elif "NUMBER" in data_type or "ID" in col_name:
            col_dataframe_names[col_name] = [fake.ID_provider() for ID in range(size)]
        elif "COUNTRY" in col_name:
            col_dataframe_names[col_name] = [fake.country() for ID in range(size)]
        else:
            col_dataframe_names[col_name] = [fake.color_name() for ID in range(size)]
    # Geting Column Names
    columns = list(col_dataframe_names.keys())
    # Getting Columns with Values
    data = [[*vals] for vals in zip(*col_dataframe_names.values())]
    # Putting Into a DataFrame
    # df_1 = spark.createDataFrame(data, columns)
    df_1 = pd.DataFrame(data = data, columns = columns)
    # df_1 = pd.createDataFrame(data=data, index=columns)
    # print(data)
    return df_1

if __name__ == '__main__':
    create_dataframe()