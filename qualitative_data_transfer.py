import os
import pyodbc
import logging
import pandas as pd
import json
from datetime import datetime
import time
import logging
import uuid
from dotenv import load_dotenv
load_dotenv('local_config.env')

def azure_sql_connection(client, source = False, destination = False):
    if source:
        connection_string = os.environ["DB_CONNECTION_STRING_TEMPLATE_SOURCE"].replace("{{client_code}}", client)
    elif destination:
        connection_string = os.environ["DB_CONNECTION_STRING_TEMPLATE_DESTINATION"].replace("{{client_code}}", client)
    cnxn = None
    try:
        # conn_engine = create_engine('mssql+pyodbc://'+username+':'+password+'@'+server+':1433'+'/'+database+'?driver=ODBC+Driver+17+for+SQL+Server')
        cnxn = pyodbc.connect(connection_string)
        print(f"Connected to azure sql database. Client : {client}")
    except Exception as err:
        print(f"Failed to connect to azure sql database - {err}")
        raise ConnectionError
    return cnxn

def execute_data_queries(query, cnxn, params=None, frame_columns=None):
    # results = cnxn.execute(query)
    cursor = cnxn.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    if frame_columns is None:
        frame_columns=[col[0] for col in cursor.description]
    records = cursor.fetchall()
    df = pd.DataFrame.from_records(records, columns=frame_columns)
    df_shape = df.shape
    if df_shape[0] > 0:
        df.columns = frame_columns
    else:
        return pd.DataFrame(columns = frame_columns)
    cursor.close()
    return df

def insert_update_data_queries(query, cnxn, params=None):
    cursor = cnxn.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    cnxn.commit()
    time.sleep(5)
    cursor.close()

def get_firm_forms(destination_cnxn):
    get_form_query = " select ID, Type from FirmInfoSectionTypes where isactive = 1 and Type not in ('BiosToDisplay', 'Teams', 'KeyPersonnel','KeyInvestmentProfessionals')"
    firm_forms = execute_data_queries(get_form_query, destination_cnxn)
    return firm_forms
    

def get_firm_data_from_source(source_cnxn):
    firm_update_history_query = "select FirmInfoSectionTypeId, fst.Type as SectionType,  DataJSON from FirmInfoUpdateHistory fuh inner join FirmInfoSectionTypes fst on fst.id = fuh.FirmInfoSectionTypeId where fuh.IsActive = 1"
    firm_data = execute_data_queries(firm_update_history_query, source_cnxn)
    return firm_data

def get_existing_firm_data_at_destination(destination_cnxn):
    firm_info_query = "select Id, FirmInfoSectionTypeId from FirmInfo where isactive = 1"
    firm_info_data = execute_data_queries(firm_info_query, destination_cnxn)

    firm_update_info_query = "select FirmInfoId, EffectiveDate from FirmInfoUpdateHistory where isactive = 1"
    firm_update_info_data = execute_data_queries(firm_update_info_query, destination_cnxn)

    return firm_info_data, firm_update_info_data


def get_firm_reference_data(destination_cnxn):
    user_id_query = "select ID from Users where Loginname = 'ManeeshaP'"
    user_id_df = execute_data_queries(user_id_query, destination_cnxn)
    user_id = int(user_id_df['ID'].values[0])

    qualitative_input_strucure_query = "select Id, Name, BaseStructureId from QualitativeInputStructure where IsActive = 1"
    qualitative_input_strucure_data = execute_data_queries(qualitative_input_strucure_query, destination_cnxn)

    return user_id, qualitative_input_strucure_data

def get_new_id_for_firm_data(destination_cnxn, section_type_id):
    get_new_id_query = "select Id from FirmInfo where FirmInfoSectionTypeId = ? and isactive = 1"
    new_id_df = execute_data_queries(get_new_id_query, destination_cnxn, (section_type_id,))
    new_id = int(new_id_df['Id'].values[0])
    return new_id

def get_strategy_forms(destination_cnxn):
    get_form_query = " select ID, Type from StrategyInfoSectionTypes where isactive = 1 and Type not in ('Teams','Product')"
    strategy_forms = execute_data_queries(get_form_query, destination_cnxn)
    return strategy_forms
    

def get_strategy_data_from_source(source_cnxn, strategy_code):
    strategy_update_history_query = f"select sist.Type, sisu.DataJSON from StrategyInfoSectionsUpdateHistory sisu \
inner join StrategyInfoSections sis on sis.id = sisu.StrategyInfoSectionId \
inner join StrategyInfo sinfo on sinfo.Id = sis.StrategyInfoId \
inner join (select * from attributetypevalues where attributetypeid = 1 and IsActive = 1) atv on atv.id = sinfo.StrategyId \
inner join StrategyInfoSectionTypes sist on sist.Id = sisu.StrategyInfoSectionTypeId \
where sisu.isactive = 1 and atv.Value = '{strategy_code}'"
    strategy_data = execute_data_queries(strategy_update_history_query, source_cnxn)
    return strategy_data

def get_strategy_reference_data(destination_cnxn, strategy_code):
    user_id_query = "select ID from Users where Loginname = 'ManeeshaP'"
    user_id_df = execute_data_queries(user_id_query, destination_cnxn)
    user_id = int(user_id_df['ID'].values[0])

    qualitative_input_strucure_query = "select Id, Name, BaseStructureId from QualitativeInputStructure where IsActive = 1"
    qualitative_input_strucure_data = execute_data_queries(qualitative_input_strucure_query, destination_cnxn)

    strategy_id_query = f"select Id from AttributeTypeValues where Value = '{strategy_code}'"
    strategy_id_df = execute_data_queries(strategy_id_query, destination_cnxn)
    strategy_id = int(strategy_id_df['Id'].values[0])

    return user_id, qualitative_input_strucure_data, strategy_id

def get_existing_strategy_data_at_destination(destination_cnxn, strategy_code):
    strategy_info_query = f"select si.Id, StrategyId from StrategyInfo si inner join attributetypevalues atv on atv.id = si.StrategyID where atv.Value = '{strategy_code}'"
    strategy_info_data = execute_data_queries(strategy_info_query, destination_cnxn)

    if strategy_info_data.shape[0] > 0:
        strategy_info_id = int(strategy_info_data['Id'].values[0])
        strategy_info_sections_data_query = f"select id, StrategyInfoId, StrategyInfoSectionTypeId, EffectiveDate from StrategyInfoSections where isactive = 1 and StrategyInfoId = {str(strategy_info_id)}"
        strategy_info_sections_data = execute_data_queries(strategy_info_sections_data_query, destination_cnxn)
    else:
        strategy_info_sections_data = pd.DataFrame(columns=['Id', 'StrategyInfoId', 'StrategyInfoSectionTypeId', 'EffectiveDate'])
    
    # if strategy_info_sections_data.shape[0] > 0:
    #     strategy_info_section_ids = list(strategy_info_sections_data['Id'])
    #     strategy_info_section_update_history = f"Select Id, StrategyInfoSectionId, StrategyInfoSectionTypeId, EffectiveDate from StrategyInfoSectionsUpdateHistory where isactive = 1 and StrategyInfoSectionId in ({','.join([str(i) for i in strategy_info_section_ids])})"
    #     strategy_info_section_update_history_data = execute_data_queries(strategy_info_section_update_history, destination_cnxn)
    # else:
    #     strategy_info_section_update_history_data = pd.DataFrame()

    return strategy_info_data, strategy_info_sections_data

def get_new_strategyInfoSectionId(destination_cnxn, strategy_info_id, strategy_info_section_type_id):
    get_new_id_query = "select Id from StrategyInfoSections where StrategyInfoId = ? and StrategyInfoSectionTypeId = ? and isactive = 1"
    new_id_df = execute_data_queries(get_new_id_query, destination_cnxn, (strategy_info_id, strategy_info_section_type_id))
    new_id = int(new_id_df['Id'].values[0])
    return new_id


def orchestrate_data_transfer(client_code, firm = False, strategy = False, strategy_list = []):
    source_cnxn = azure_sql_connection(client_code, source = True)
    destination_cnxn = azure_sql_connection(client_code, destination = True)
    if firm:
        firm_forms = get_firm_forms(destination_cnxn)
        source_data  = get_firm_data_from_source(source_cnxn)
        user_id, qualitative_input_strucure_data = get_firm_reference_data(destination_cnxn)
        existing_firm_info_data, firm_update_info_data = get_existing_firm_data_at_destination(destination_cnxn)

        for row_index, row_data in firm_forms.iterrows():
            form_name = row_data['Type']
            form_data = source_data[source_data['SectionType'] == form_name]
            if form_data.shape[0] > 0:
                # Construct insert statement to FirmInfo
                firm_section_type_id = int(row_data['ID'])
                effective_date = '2024-04-01'
                created_by = user_id
                updated_by = user_id
                created_at = '2024-07-24'
                updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                data_status = 3
                locale_attribute_id = 16
                base_structure_id = int(qualitative_input_strucure_data[qualitative_input_strucure_data['Name'] == "FirmInfo"+form_name]['BaseStructureId'].values[0])
                # check if data exists for the form already
                firm_info_data_for_form = existing_firm_info_data[existing_firm_info_data['FirmInfoSectionTypeId'] == firm_section_type_id]
                existing_id = None
                if firm_info_data_for_form.shape[0] > 0:
                    existing_id = int(firm_info_data_for_form['Id'].values[0])

                if existing_id:
                    update_sql = f"update FirmInfo set isactive = 0 where Id = {str(existing_id)}"
                    print(update_sql)
                    insert_update_data_queries(update_sql, destination_cnxn)
                
                json_data = str(form_data['DataJSON'].values[0])
                json_data = json_data.replace("'", "''")
                insert_sql_firm_info = f"insert into FirmInfo (FirmInfoSectionTypeId, EffectiveDate, CreatedBy, UpdatedBy,CreatedAt, UpdatedAt, DataStatus, LocaleAttributeId, BaseStructureId, DataJSON,Version, IsActive) values ({str(firm_section_type_id)}, '{effective_date}', {str(created_by)}, {str(updated_by)} ,'{created_at}', '{str(updated_at)}', {str(data_status)}, {str(locale_attribute_id)}, {str(base_structure_id)}, '{json_data}', 1, 1)" 
                                
                insert_update_data_queries(insert_sql_firm_info, destination_cnxn)

                # Get the new ID for the Firm info form
                new_id = get_new_id_for_firm_data(destination_cnxn, firm_section_type_id)

                # Check if an entry exists in FirmInfoUpdateHistory
                if firm_update_info_data.shape[0] > 0:
                    update_sql_firm_info_history = f"update FirmInfoUpdateHistory set isactive = 0 where FirmInfoId = {str(existing_id)}"
                    insert_update_data_queries(update_sql_firm_info_history, destination_cnxn)

                # Construct insert statement to FirmInfoUpdateHistory
                insert_sql_firm_info_history = f"insert into FirmInfoUpdateHistory (FirmInfoId, FirmInfoSectionTypeId, EffectiveDate, UpdatedBy, UpdatedAt, DataStatus, ApprovalState, LocaleAttributeId, BaseStructureId, DataJSON,Version, IsActive) values ({str(new_id)}, {str(firm_section_type_id)}, '{effective_date}', {str(updated_by)}, '{str(updated_at)}', {str(data_status)}, 2, {str(locale_attribute_id)}, {str(base_structure_id)},  '{json_data}', 1, 1)"
                with open('insert_sql_firm_info.sql', 'w') as f:
                    f.write(insert_sql_firm_info_history)  
                insert_update_data_queries(insert_sql_firm_info_history, destination_cnxn)
            else:
                # with open('insert_sql_firm_info.sql', 'w') as f:
                #     f.write(insert_sql_firm_info)  
                logging.info(f"No data found for form {form_name}")
    
    if strategy:
        strategy_forms = get_strategy_forms(destination_cnxn)
        for strategy_code in strategy_list:
            strategy_source_data  = get_strategy_data_from_source(source_cnxn, strategy_code)
            user_id, qualitative_input_strucure_data, strategy_id = get_strategy_reference_data(destination_cnxn, strategy_code)
            strategy_info_data, strategy_info_sections_data = get_existing_strategy_data_at_destination(destination_cnxn, strategy_code)

            # check if strategy info table has an entry for the strategy
            if not strategy_info_data.shape[0] > 0:
                # Construct insert statement to StrategyInfo
                created_by = user_id
                updated_by = user_id
                created_at = '2024-07-31'
                updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                insert_sql_strategy_info = f"insert into StrategyInfo (StrategyId, CreatedBy, CreatedAt, UpdatedBy, UpdatedAt) values ({str(strategy_id)}, {str(created_by)}, '{str(created_at)}', {str(updated_by)} , '{str(updated_at)}')" 
                insert_update_data_queries(insert_sql_strategy_info, destination_cnxn)

                # Get the new ID for the Strategy info form
                strategy_info_data, strategy_info_sections_data = get_existing_strategy_data_at_destination(destination_cnxn, strategy_code)

            for row_index, row_data in strategy_forms.iterrows():
                form_name = row_data['Type']
                print("Updating form : ", form_name)
                form_data = strategy_source_data[strategy_source_data['Type'] == form_name]

                if form_data.shape[0] > 0:
                    # Construct insert statement to StrategyInfoSections
                    
                    strategy_info_section_type_id = int(row_data['ID'])

                    # check if data exists for the form already
                    strategy_info_data_for_form = strategy_info_sections_data[strategy_info_sections_data['StrategyInfoSectionTypeId'] == strategy_info_section_type_id]
                    strategy_info_sections_existing_id = None
                    if strategy_info_data_for_form.shape[0] > 0:
                        strategy_info_sections_existing_id = int(strategy_info_data_for_form['id'].values[0])

                    if strategy_info_sections_existing_id:
                        update_sql = f"update StrategyInfoSections set isactive = 0 where Id = {str(strategy_info_sections_existing_id)}"
                        insert_update_data_queries(update_sql, destination_cnxn)
                        update_sql_strategy_info_history = f"update StrategyInfoSectionsUpdateHistory set isactive = 0 where StrategyInfoSectionId = {str(strategy_info_sections_existing_id)}"
                        insert_update_data_queries(update_sql_strategy_info_history, destination_cnxn)
                    
                    strategy_info_id = int(strategy_info_data['Id'].values[0])
                    json_data = str(form_data['DataJSON'].values[0])
                    json_data = json_data.replace("'", "''")
                    effective_date = '2024-04-01'
                    created_by = user_id
                    created_at = '2024-07-31'

                    updated_by = user_id
                    updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    data_status = 3
                    locale_attribute_id = 16
                    strategy_section_id = uuid.uuid4()
                    base_structure_id = int(qualitative_input_strucure_data[qualitative_input_strucure_data['Name'] == "StrategyInfo"+form_name]['BaseStructureId'].values[0])
                    
                    strategy_info_sections_insert_query = f"Insert into StrategyInfoSections (StrategyInfoId, StrategyInfoSectionTypeId, DataJSON, EffectiveDate, CreatedBy, CreatedAt, UpdatedBy, UpdatedAt, DataStatus, LocaleAttributeId, StrategySectionId, BaseStructureId, Version, IsActive) values ({str(strategy_info_id)}, {str(strategy_info_section_type_id)}, '{json_data}', '{str(effective_date)}', {str(created_by)}, '{str(created_at)}', {str(updated_by)}, '{str(updated_at)}', {str(data_status)},{str(locale_attribute_id)},'{str(strategy_section_id)}', {str(base_structure_id)}, 1, 1)"
                    insert_update_data_queries(strategy_info_sections_insert_query, destination_cnxn)

                    # Insert into StrategyInfoSectionsUpdateHistory
                    new_strategy_info_section_id = get_new_strategyInfoSectionId(destination_cnxn, strategy_info_id, strategy_info_section_type_id)
                    strategy_info_section_update_history_insert_query = f"Insert into StrategyInfoSectionsUpdateHistory (StrategyInfoSectionId, StrategyInfoSectionTypeId, DataJSON, EffectiveDate, UpdatedBy, UpdatedAt, DataStatus, ApprovalState, LocaleAttributeId, StrategySectionId,BaseStructureId, Version, IsActive) values ({str(new_strategy_info_section_id)}, {str(strategy_info_section_type_id)}, '{json_data}', '{effective_date}', {str(updated_by)}, '{str(updated_at)}', {str(data_status)}, 2, {str(locale_attribute_id)}, '{str(strategy_section_id)}',{str(base_structure_id)}, 1, 1)"
                    insert_update_data_queries(strategy_info_section_update_history_insert_query, destination_cnxn)


if __name__ == "__main__":
    orchestrate_data_transfer('BYOD', firm = False, strategy = True, strategy_list = ['GVE'])
