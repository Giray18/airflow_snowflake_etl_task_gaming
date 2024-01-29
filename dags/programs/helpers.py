
def helpers():
    from faker import Faker
    from faker.providers import DynamicProvider

    ## Creating raw data tables dicts consisting of column names and datatypes #######
    in_app_purchase_log_server_columns = ['EVENT_TIMESTAMP','EVENT_UUID','HC_PRODUCT_VALUE','HC_PRODUCT_VALUE_BONUS',
                                        'IN_APP_PURCHASE_ID','IS_SANDBOX','PAYMENT_COUNTRY','PLATFORM','PLATFORM_PURCHASE_KEY',
                                        'PRODUCT_NAME','REAL_CURRENCY_AMOUNT','REAL_CURRENCY_TYPE','SESSION_ID','USD_COST',
                                        'USER_ID','USER_IS_PREMIUM','USER_IS_SPENDER','USER_LEVEL','VALIDATED','USER_REGION',
                                        'SERVER_VERSION','CLIENT_VERSION','USER_GEO','USER_TYPE','USER_CLAN_ID','FREE_XP_AMOUNT',
                                        'SILVER_AMOUNT','GOLD_AMOUNT','PRE_SET_ITEM_ID','IAP_ID','ITEM_IDS','ALIAS',
                                        'USER_INGAME_ID','EVENT_ID','USER_CLAN_LEVEL','IS_FREETRIAL','STORE']

    in_app_purchase_log_server_columns_type = ['TIMESTAMP_TZ','VARCHAR','NUMBER','NUMBER','VARCHAR','BOOLEAN','VARCHAR','VARCHAR',
                                            'VARCHAR','VARCHAR','NUMBER','VARCHAR','VARCHAR','NUMBER','VARCHAR','BOOLEAN',
                                            'BOOLEAN','NUMBER','BOOLEAN','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR',
                                            'VARCHAR','NUMBER','NUMBER','NUMBER','VARCHAR','VARCHAR','VARCHAR','VARCHAR',
                                            'VARCHAR','VARCHAR','NUMBER','VARCHAR','VARCHAR']

    in_app_purchase_log_server_dict = dict(map(lambda i,j : (i,j) , in_app_purchase_log_server_columns,in_app_purchase_log_server_columns_type))



    login_columns = ['EVENT_ID','EVENT_TIMESTAMP','EVENT_UUID','LOGIN_AUTHENTICATION_PLATFORM','LOGIN_SUCCESSFUL',
    'MAIN_EVENT_ID','MS_SINCE_LAST_EVENT','PLATFORM','SDK_VERSION','SERVER_CLUSTER','SESSION_ID',
    'USER_ID','USER_IS_PREMIUM','USER_IS_SPENDER','USER_LEVEL','USER_LTV','USER_SCORE','USER_SERVER',
    'USER_XP','ROW_NUMBER','DEVICE_ID','LOGIN_ATTEMPT_ID','USER_REGION','SERVER_VERSION',
    'CLIENT_VERSION','USER_GEO','USER_TYPE','PLAYER_TYPE','USER_CLAN_ID','USER_INGAME_ID',
    'USER_CLAN_LEVEL','GDPR_PROCESSED_TS','SC_BALANCE','HC_BALANCE','FREE_XP_BALANCE',
    'LEAGUE_RATING_BALANCE','STORE']


    login_column_types = ['NUMBER','TIMESTAMP_TZ','VARCHAR','VARCHAR','BOOLEAN','NUMBER','DOUBLE',
                        'VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','BOOLEAN','BOOLEAN',
                        'NUMBER','NUMBER','NUMBER','VARCHAR','NUMBER','NUMBER','VARCHAR','VARCHAR',
                        'VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR',
                        'VARCHAR','NUMBER','TIMESTAMP_TZ','NUMBER','NUMBER',
                        'NUMBER','NUMBER','VARCHAR']


    login_dict = dict(map(lambda i,j : (i,j) , login_columns,login_column_types))


    multiplayer_battle_started_columns = ['CLAN_ID','CLAN_LEVEL','CLAN_NAME','CLAN_RANK','COLLECT_INSERTED_TIMESTAMP',
        'EVENT_ID','EVENT_TIMESTAMP','EVENT_UUID','MAIN_EVENT_ID','MAP_ID','MAP_NAME',
        'MS_SINCE_LAST_EVENT','MULTIPLAYER_BATTLE_ID','MULTIPLAYER_BATTLE_MODE',
        'MULTIPLAYER_BATTLE_TEAM','MULTIPLAYER_BATTLE_TYPE','PLATFORM','SDK_VERSION','SESSION_ID',
        'SHIP_AIR_DEFENSE_LEVEL','SHIP_ANTI_AIR','SHIP_BOMBER_LEVEL','SHIP_CAMOUFLAGE_ID',
        'SHIP_CAMOUFLAGE_NAME','SHIP_CARRIER_AIR_GROUP','SHIP_CONCEALMENT','SHIP_CONSUMABLE_1',
        'SHIP_CONSUMABLE_2','SHIP_DEFENSE_LEVEL','SHIP_ELITE','SHIP_ELITE_ATTRIBUTE_ID',
        'SHIP_ELITE_ATTRIBUTE_NAME','SHIP_EQUIPMENT_1','SHIP_EQUIPMENT_2','SHIP_EQUIPMENT_3',
        'SHIP_FIGHTERS_LEVEL','SHIP_FIRE_CONTROL_LEVEL','SHIP_GUNS','SHIP_HULL_LEVEL','SHIP_ID',
        'SHIP_MAIN_GUNS_LEVEL','SHIP_MOBILITY','SHIP_NAME','SHIP_NATION','SHIP_PREMIUM',
        'SHIP_SECONDARY_GUNS_LEVEL','SHIP_SKILL_NAME','SHIP_SUPPLY_1','SHIP_SUPPLY_2',
        'SHIP_SUPPLY_3','SHIP_SURVIVABILITY','SHIP_TIER','SHIP_TORPEDO','SHIP_TORPEDOBOMBERS_LEVEL',
        'SHIP_TORPEDOES_LEVEL','SHIP_TYPE','USER_ID','USER_IS_PREMIUM','USER_IS_SPENDER','USER_LEVEL',
        'USER_LTV','USER_SCORE','USER_SERVER','USER_XP','SHIP_TORPEDO_BOMBERS_LEVEL','SHIP_HULL__LEVEL',
        'SHIP_EQUIPMENT_1_1','SHIP_EQUIPMENT_2_1','USER_SERVER_1','SHIP_EQUIPMENT_3_1','SERVER_VERSION',
        'USER_REGION','SHIP_SKILL1_NAME','USER_TYPE','USER_GEO','SHIP_SKILL2_NAME','CLIENT_VERSION',
        'PLAYER_TYPE','SHIP_BOOST_1','SHIP_BOOST_2','USER_CLAN_ID','RANKED_DEVISION','RANKED_SEASON_ID',
        'USER_INGAME_ID','SHIP_SKILL3_NAME','TITLE_USED','USER_CLAN_LEVEL','PORTRAIT_NAME','CHALLENGE_ID_3',
        'SHIP_LIMITED','CHALLENGE_ID_1','CHALLENGE_ID_2','TEAM_ID','STORE']


    multiplayer_battle_started_column_types = ['VARCHAR','NUMBER','VARCHAR','NUMBER','TIMESTAMP_TZ','NUMBER',
                                            'TIMESTAMP_TZ','VARCHAR','NUMBER','VARCHAR','VARCHAR','DOUBLE',
                                            'VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR',
                                            'NUMBER','NUMBER','NUMBER','VARCHAR','VARCHAR','NUMBER','NUMBER',
                                            'VARCHAR','VARCHAR','NUMBER','BOOLEAN','VARCHAR','VARCHAR','VARCHAR',
                                            'VARCHAR','VARCHAR','NUMBER','NUMBER','NUMBER','NUMBER','VARCHAR',
                                            'NUMBER','NUMBER','VARCHAR','VARCHAR','BOOLEAN','NUMBER','VARCHAR',
                                            'VARCHAR','VARCHAR','VARCHAR','NUMBER','NUMBER','NUMBER','NUMBER',
                                            'NUMBER','VARCHAR','VARCHAR','BOOLEAN','BOOLEAN','NUMBER','NUMBER',
                                            'NUMBER','VARCHAR','NUMBER','NUMBER','NUMBER','NUMBER','NUMBER','NUMBER',
                                            'NUMBER','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR',
                                            'VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR',
                                            'VARCHAR','VARCHAR','VARCHAR','NUMBER','VARCHAR','VARCHAR','NUMBER',
                                            'VARCHAR','VARCHAR','VARCHAR','VARCHAR']

    multiplayer_battle_started_dict = dict(map(lambda i,j : (i,j) , multiplayer_battle_started_columns,multiplayer_battle_started_column_types))

    new_user_columns = ['BIRTH_YEAR','CAMPAIGN_INSTANCE','COLLECT_INSERTED_TIMESTAMP','EMAIL','EVENT_ID',
    'EVENT_TIMESTAMP','EVENT_UUID','GENDER','MAIN_EVENT_ID','MS_SINCE_LAST_EVENT',
    'PLATFORM','SDK_VERSION','SESSION_ID','USER_COUNTRY','USER_ID','USER_LEVEL','USER_NAME',
    'USER_SCORE','USER_SERVER','USER_XP','APPS_FLYER_ID','USER_REGION','SERVER_VERSION',
    'USER_GEO','CLIENT_VERSION','USER_CLAN_ID','USER_TYPE','USER_CLAN_LEVEL','GDPR_PROCESSED_TS',
    'USER_INGAME_ID','STORE']


    new_user_column_types = ['TIMESTAMP_TZ','VARCHAR','TIMESTAMP_TZ','VARCHAR','NUMBER','TIMESTAMP_TZ',
                            'VARCHAR','VARCHAR','NUMBER','DOUBLE','VARCHAR','VARCHAR','VARCHAR',
                            'VARCHAR','VARCHAR','NUMBER','VARCHAR','NUMBER','VARCHAR','NUMBER',
                            'VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR',
                            'NUMBER','TIMESTAMP_TZ','VARCHAR','VARCHAR']

    new_user_dict = dict(map(lambda i,j : (i,j) , new_user_columns,new_user_column_types))


    session_started_columns = ['ANDROID_REGISTRATION_ID','COLLECT_INSERTED_TIMESTAMP','EVENT_ID','EVENT_TIMESTAMP',
    'EVENT_UUID','MAIN_EVENT_ID','MS_SINCE_LAST_EVENT','PLATFORM','SDK_VERSION','SESSION_ID',
    'USER_GEO_LOCATION','USER_ID','USER_LEVEL','USER_SCORE','USER_SERVER','USER_XP',
    'APPS_FLYER_ID','USER_IS_SPENDER','DEVICE_ID','USER_IS_PREMIUM','USER_REGION','SERVER_VERSION',
    'USER_TYPE','USER_GEO','PLAYER_TYPE','CLIENT_VERSION','USER_CLAN_ID','USER_INGAME_ID',
    'USER_CLAN_LEVEL','GDPR_PROCESSED_TS','STORE']

    session_started_column_types = ['VARCHAR','TIMESTAMP_TZ','NUMBER','TIMESTAMP_TZ','VARCHAR',
                                    'NUMBER','DOUBLE','VARCHAR','VARCHAR','VARCHAR','VARCHAR',
                                    'VARCHAR','NUMBER','NUMBER','VARCHAR','NUMBER','VARCHAR',
                                    'BOOLEAN','VARCHAR','BOOLEAN','VARCHAR','VARCHAR','VARCHAR',
                                    'VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','NUMBER',
                                    'TIMESTAMP_TZ','VARCHAR']


    session_started_dict = dict(map(lambda i,j : (i,j) , session_started_columns,session_started_column_types))


    ship_transaction_log_columns = ['COLLECT_INSERTED_TIMESTAMP','EVENT_ID','EVENT_TIMESTAMP','EVENT_UUID','HC_AMOUNT',
    'MAIN_EVENT_ID','MS_SINCE_LAST_EVENT','PLATFORM','SC_AMOUNT','SDK_VERSION','SESSION_ID',
    'SHIP_ID','SHIP_NAME','SHIP_NATION','SHIP_PREMIUM','SHIP_TIER','SHIP_TYPE','TRANSACTION_ID',
    'USER_ID','USER_IS_PREMIUM','USER_IS_SPENDER','USER_LEVEL','USER_LTV','USER_SCORE',
    'USER_SERVER','USER_XP','SERVER_VERSION','USER_REGION','USER_TYPE','USER_GEO','PLAYER_TYPE',
    'CLIENT_VERSION','USER_CLAN_ID','IN_APP_PURCHASE_ID','REASON','REASON_EXTRA','RANKED_COIN_AMOUNT',
    'USER_INGAME_ID','USER_CLAN_LEVEL','GDPR_PROCESSED_TS','FREE_XP_AMOUNT','IS_FREE_GIFT',
    'SHIP_LIMITED','STORE']

    ship_transaction_log_column_types = ['TIMESTAMP_TZ','NUMBER','TIMESTAMP_TZ','VARCHAR','NUMBER',
                                        'NUMBER','DOUBLE','VARCHAR','NUMBER','VARCHAR','VARCHAR',
                                        'VARCHAR','VARCHAR','VARCHAR','BOOLEAN','NUMBER','VARCHAR',
                                        'VARCHAR','VARCHAR','BOOLEAN','BOOLEAN','NUMBER','NUMBER',
                                        'NUMBER','VARCHAR','NUMBER','VARCHAR','VARCHAR','VARCHAR',
                                        'VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR','VARCHAR',
                                        'VARCHAR','NUMBER','VARCHAR','NUMBER','TIMESTAMP_TZ','NUMBER',
                                        'BOOLEAN','NUMBER','VARCHAR']

    ship_transaction_log_dict = dict(map(lambda i,j : (i,j) , ship_transaction_log_columns,ship_transaction_log_column_types))


    #### Creating dataframe/table names list dict to be used in loop actions to create dataframes or operations on hive.metastore write actions

    table_names_list = {"in_app_purchase_df": in_app_purchase_log_server_dict, "login_df" : login_dict, 
                        "multiplayer_battle_df": multiplayer_battle_started_dict, 
                        "new_user_df" : new_user_dict, 
                        "session_started_df" : session_started_dict,
                        "ship_transaction_df" : ship_transaction_log_dict}


    # Creating Silver layer column name holding dicts to read only necessarry columns from bronze layer 

    silver_layer_col_names = {"in_app_purchase_df": ['EVENT_TIMESTAMP','HC_PRODUCT_VALUE','HC_PRODUCT_VALUE_BONUS','EVENT_ID',
                            'IN_APP_PURCHASE_ID','IS_SANDBOX','PAYMENT_COUNTRY','PLATFORM','PLATFORM_PURCHASE_KEY',
                            'PRODUCT_NAME','REAL_CURRENCY_AMOUNT','REAL_CURRENCY_TYPE','USD_COST',
                            'FREE_XP_AMOUNT','SILVER_AMOUNT','GOLD_AMOUNT','IS_FREETRIAL','USER_ID','SESSION_ID','JOIN_KEY'],

                            "login_df" : ['EVENT_ID','EVENT_TIMESTAMP','EVENT_UUID','LOGIN_AUTHENTICATION_PLATFORM','LOGIN_SUCCESSFUL',
                            'PLATFORM','SDK_VERSION','SERVER_CLUSTER','ROW_NUMBER','DEVICE_ID','LOGIN_ATTEMPT_ID','SERVER_VERSION',
                            'CLIENT_VERSION','USER_ID','SESSION_ID','JOIN_KEY'],
                            
                            "session_started_df" : ['ANDROID_REGISTRATION_ID','EVENT_ID','EVENT_TIMESTAMP',
                            'EVENT_UUID','SESSION_ID',
                            'USER_ID','USER_GEO_LOCATION','USER_IS_SPENDER','DEVICE_ID','SERVER_VERSION',
                            'CLIENT_VERSION','JOIN_KEY'],

                            "new_user_df" : ['BIRTH_YEAR','EMAIL','EVENT_TIMESTAMP','EVENT_UUID','EVENT_ID',
                            'GENDER','USER_COUNTRY','USER_ID','USER_LEVEL','USER_NAME','USER_SCORE',
                            'USER_SERVER','USER_XP','USER_REGION','USER_GEO',
                            'USER_CLAN_ID','USER_TYPE','USER_CLAN_LEVEL','USER_INGAME_ID','SESSION_ID','JOIN_KEY'],

                            "multiplayer_battle_df" : [
                            'EVENT_ID','EVENT_TIMESTAMP','EVENT_UUID','MAP_ID','MAP_NAME',
                            'MULTIPLAYER_BATTLE_ID','MULTIPLAYER_BATTLE_MODE',
                            'MULTIPLAYER_BATTLE_TEAM','MULTIPLAYER_BATTLE_TYPE','CHALLENGE_ID_3',
                            'CHALLENGE_ID_1','CHALLENGE_ID_2','TEAM_ID','USER_ID','SESSION_ID','JOIN_KEY'],

                            "ship_transaction_df" : ['EVENT_ID','EVENT_TIMESTAMP','EVENT_UUID',
                            'SC_AMOUNT','SESSION_ID',
                            'SHIP_ID','SHIP_NAME','SHIP_NATION','SHIP_PREMIUM','SHIP_TIER','SHIP_TYPE','TRANSACTION_ID',
                            'USER_ID','IS_FREE_GIFT',
                            'SHIP_LIMITED','JOIN_KEY'] }

    silver_layer_col_names_1 = {"user_id_df" : ['USER_ID','USER_IS_SPENDER','JOIN_KEY']}


    # # ## Golden Layer Table list

    golden_layer_read_table_names_list = ['in_app_purchase_df_silver_layer_managed_table', 'login_df_silver_layer_managed_table', 'multiplayer_battle_df_silver_layer_managed_table', 'new_user_df_silver_layer_managed_table', 'session_started_df_silver_layer_managed_table', 'ship_transaction_df_silver_layer_managed_table', 'user_id_df_silver_layer_managed_table']

    # # ## Golden Layer Column Rename Mapping Dict
    # golden_layer_col_names = {"d_in_app_purchase": ['EVENT_TIMESTAMP','IN_APP_EVENT_TIMESTAMP','HC_PRODUCT_VALUE','IN_APP_HC_PRODUCT_VALUE','HC_PRODUCT_VALUE_BONUS','IN_APP_HC_PRODUCT_VALUE_BONUS','EVENT_ID','IN_APP_EVENT_ID',
    #                         'IN_APP_PURCHASE_ID','IN_APP_PURCHASE_ID','IS_SANDBOX','IN_APP_IS_SANDBOX','PAYMENT_COUNTRY','IN_APP_PAYMENT_COUNTRY','PLATFORM','IN_APP_PLATFORM','PLATFORM_PURCHASE_KEY','IN_APP_PLATFORM_PURCHASE_KEY',
    #                         'PRODUCT_NAME','IN_APP_PRODUCT_NAME','REAL_CURRENCY_AMOUNT','IN_APP_REAL_CURRENCY_AMOUNT','REAL_CURRENCY_TYPE','IN_APP_REAL_CURRENCY_TYPE','USD_COST','IN_APP_USD_COST','FREE_XP_AMOUNT',
    #                         'IN_APP_FREE_XP_AMOUNT','SILVER_AMOUNT','IN_APP_SILVER_AMOUNT','GOLD_AMOUNT','IN_APP_GOLD_AMOUNT','IS_FREETRIAL','IN_APP_IS_FREETRIAL','USER_ID','IN_APP_USER_ID','SESSION_ID','IN_APP_SESSION_ID','Join_Key','Join_Key'], 

    #                         "d_login" : ['EVENT_ID','LOGIN_EVENT_ID','EVENT_TIMESTAMP','LOGIN_EVENT_TIMESTAMP','EVENT_UUID','LOGIN_EVENT_UUID','LOGIN_AUTHENTICATION_PLATFORM','LOGIN_AUTHENTICATION_PLATFORM','LOGIN_SUCCESSFUL','LOGIN_SUCCESSFUL',
    #                         'PLATFORM','LOGIN_PLATFORM','SDK_VERSION','LOGIN_SDK_VERSION','SERVER_CLUSTER','LOGIN_SERVER_CLUSTER','ROW_NUMBER','LOGIN_ROW_NUMBER','DEVICE_ID','LOGIN_DEVICE_ID','LOGIN_ATTEMPT_ID','LOGIN_ATTEMPT_ID','SERVER_VERSION','LOGIN_SERVER_VERSION',
    #                         'CLIENT_VERSION','LOGIN_CLIENT_VERSION','USER_ID','LOGIN_USER_ID','SESSION_ID','LOGIN_SESSION_ID','Join_Key','Join_Key'],
                            
    #                         "d_session_started" : ['ANDROID_REGISTRATION_ID','SESSION_ANDROID_REGISTRATION_ID','EVENT_ID','SESSION_EVENT_ID','EVENT_TIMESTAMP','SESSION_EVENT_TIMESTAMP',
    #                         'EVENT_UUID','SESSION_EVENT_UUID','SESSION_ID','SESSION_SESSION_ID',
    #                         'USER_ID','SESSION_USER_ID','USER_GEO_LOCATION','SESSION_USER_GEO_LOCATION','USER_IS_SPENDER','SESSION_USER_IS_SPENDER','DEVICE_ID','SESSION_DEVICE_ID','SERVER_VERSION','SESSION_SERVER_VERSION',
    #                         'CLIENT_VERSION','SESSION_CLIENT_VERSION','Join_Key','Join_Key'],

    #                         "d_new_user" : ['BIRTH_YEAR','USER_BIRTH_YEAR','EMAIL','USER_EMAIL','EVENT_TIMESTAMP','USER_EVENT_TIMESTAMP','EVENT_UUID','USER_EVENT_UUID','EVENT_ID','USER_EVENT_ID',
    #                         'GENDER','USER_GENDER','USER_COUNTRY','USER_COUNTRY','USER_ID','USER_USER_ID','USER_LEVEL','USER_LEVEL','USER_NAME','USER_NAME','USER_SCORE','USER_SCORE',
    #                         'USER_SERVER','USER_SERVER','USER_XP','USER_XP','USER_REGION','USER_REGION','USER_GEO','USER_GEO',
    #                         'USER_CLAN_ID','USER_CLAN_ID','USER_TYPE','USER_TYPE','USER_CLAN_LEVEL','USER_CLAN_LEVEL','USER_INGAME_ID','USER_INGAME_ID','SESSION_ID','USER_SESSION_ID','Join_Key','Join_Key'],

    #                         "d_multiplayer_battle" : [
    #                         'EVENT_ID','MULTIPLAYER_EVENT_ID','EVENT_TIMESTAMP','MULTIPLAYER_EVENT_TIMESTAMP','EVENT_UUID','MULTIPLAYER_EVENT_UUID','MAP_ID','MULTIPLAYER_MAP_ID','MAP_NAME','MULTIPLAYER_MAP_NAME',
    #                         'MULTIPLAYER_BATTLE_ID','MULTIPLAYER_BATTLE_ID','MULTIPLAYER_BATTLE_MODE','MULTIPLAYER_BATTLE_MODE',
    #                         'MULTIPLAYER_BATTLE_TEAM','MULTIPLAYER_BATTLE_TEAM','MULTIPLAYER_BATTLE_TYPE','MULTIPLAYER_BATTLE_TYPE','CHALLENGE_ID_3','MULTIPLAYER_CHALLENGE_ID_3',
    #                         'CHALLENGE_ID_1','MULTIPLAYER_CHALLENGE_ID_1','CHALLENGE_ID_2','MULTIPLAYER_CHALLENGE_ID_2','TEAM_ID','MULTIPLAYER_TEAM_ID','USER_ID','MULTIPLAYER_USER_ID','SESSION_ID','MULTIPLAYER_SESSION_ID','Join_Key','Join_Key'],

    #                         "d_ship_transaction" : ['EVENT_ID','SHIP_TRANS_EVENT_ID','EVENT_TIMESTAMP','SHIP_TRANS_EVENT_TIMESTAMP','EVENT_UUID','SHIP_TRANS_EVENT_UUID',
    #                         'SC_AMOUNT','SHIP_TRANS_SC_AMOUNT','SESSION_ID','SHIP_TRANS_SESSION_ID',
    #                         'SHIP_ID','SHIP_TRANS_SHIP_ID','SHIP_NAME','SHIP_TRANS_SHIP_NAME','SHIP_NATION','SHIP_TRANS_SHIP_NATION','SHIP_PREMIUM','SHIP_TRANS_SHIP_PREMIUM','SHIP_TIER','SHIP_TRANS_SHIP_TIER','SHIP_TYPE','SHIP_TRANS_SHIP_TYPE','TRANSACTION_ID','SHIP_TRANS_TRANSACTION_ID',
    #                         'USER_ID','SHIP_TRANS_USER_ID','IS_FREE_GIFT','SHIP_TRANS_IS_FREE_GIFT',
    #                         'SHIP_LIMITED','SHIP_TRANS_SHIP_LIMITED','Join_Key','Join_Key'],

    #                         "d_user_id" : ['USER_ID','USER_IS_SPENDER','Join_Key']

    #                             }
    

    golden_layer_col_names = {"d_in_app_purchase": ['EVENT_TIMESTAMP','HC_PRODUCT_VALUE','HC_PRODUCT_VALUE_BONUS','EVENT_ID',
                            'IN_APP_PURCHASE_ID','IS_SANDBOX','PAYMENT_COUNTRY','PLATFORM','PLATFORM_PURCHASE_KEY',
                            'PRODUCT_NAME','REAL_CURRENCY_AMOUNT','REAL_CURRENCY_TYPE','USD_COST',
                            'FREE_XP_AMOUNT','SILVER_AMOUNT','GOLD_AMOUNT','IS_FREETRIAL','USER_ID','SESSION_ID','JOIN_KEY'],

                            "d_login" : ['EVENT_ID','EVENT_TIMESTAMP','EVENT_UUID','LOGIN_AUTHENTICATION_PLATFORM','LOGIN_SUCCESSFUL',
                            'PLATFORM','SDK_VERSION','SERVER_CLUSTER','ROW_NUMBER','DEVICE_ID','LOGIN_ATTEMPT_ID','SERVER_VERSION',
                            'CLIENT_VERSION','USER_ID','SESSION_ID','JOIN_KEY'],
                            
                            "d_session_started" : ['ANDROID_REGISTRATION_ID','EVENT_ID','EVENT_TIMESTAMP',
                            'EVENT_UUID','SESSION_ID',
                            'USER_ID','USER_GEO_LOCATION','USER_IS_SPENDER','DEVICE_ID','SERVER_VERSION',
                            'CLIENT_VERSION','JOIN_KEY'],

                            "d_new_user" : ['BIRTH_YEAR','EMAIL','EVENT_TIMESTAMP','EVENT_UUID','EVENT_ID',
                            'GENDER','USER_COUNTRY','USER_ID','USER_LEVEL','USER_NAME','USER_SCORE',
                            'USER_SERVER','USER_XP','USER_REGION','USER_GEO',
                            'USER_CLAN_ID','USER_TYPE','USER_CLAN_LEVEL','USER_INGAME_ID','SESSION_ID','JOIN_KEY'],

                            "d_multiplayer_battle" : [
                            'EVENT_ID','EVENT_TIMESTAMP','EVENT_UUID','MAP_ID','MAP_NAME',
                            'MULTIPLAYER_BATTLE_ID','MULTIPLAYER_BATTLE_MODE',
                            'MULTIPLAYER_BATTLE_TEAM','MULTIPLAYER_BATTLE_TYPE','CHALLENGE_ID_3',
                            'CHALLENGE_ID_1','CHALLENGE_ID_2','TEAM_ID','USER_ID','SESSION_ID','JOIN_KEY'],
                            
                            "d_ship_transaction" : ['EVENT_ID','EVENT_TIMESTAMP','EVENT_UUID',
                            'SC_AMOUNT','SESSION_ID',
                            'SHIP_ID','SHIP_NAME','SHIP_NATION','SHIP_PREMIUM','SHIP_TIER','SHIP_TYPE','TRANSACTION_ID',
                            'USER_ID','IS_FREE_GIFT',
                            'SHIP_LIMITED','JOIN_KEY'] ,
                            
                            "d_user_id" : ['USER_ID','USER_IS_SPENDER','Join_Key']
                            
                            }


    # silver_layer_col_names_1 = {"user_id_df" : ['USER_ID','USER_GEO_LOCATION','USER_IS_SPENDER','Join_Key']}

    ##General KPI dataframes

    general_kpis_dataframes = ['f_multi_ships','d_new_user','d_user_id']

    ## Metastore_name
    # location_name = "hive_metastore"
    variable_name_list = ["in_app_purchase_log_server_columns","in_app_purchase_log_server_columns_type","in_app_purchase_log_server_dict",
                          "login_columns","login_column_types","login_dict","multiplayer_battle_started_columns",
                          "multiplayer_battle_started_column_types","multiplayer_battle_started_dict","new_user_columns",
                          "new_user_column_types","new_user_dict","session_started_columns","session_started_column_types",
                          "session_started_dict","ship_transaction_log_columns","ship_transaction_log_column_types",
                          "ship_transaction_log_dict","silver_layer_col_names","table_names_list","silver_layer_col_names_1","golden_layer_col_names"]
    
    global variable_dict
    variable_dict = {}

    for i in variable_name_list:
        variable_dict[i] = vars()[i]


    # returns all objects defined above in accessible format in a dict
    return variable_dict

if __name__ == '__main__':
    helpers()