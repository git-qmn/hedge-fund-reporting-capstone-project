from db_connection import azure_sql_connection
import pandas as pd
import json
from open_ai_interactions import interact_with_chat_application,get_openai_client_obj,interact_with_gpt4


def execute_data_queries(query, cnxn, frame_columns=None):
    cursor = cnxn.cursor()
    cursor.execute(query)
    if frame_columns is None:
        frame_columns=[col[0] for col in cursor.description]
    records = cursor.fetchall()
    df = pd.DataFrame.from_records(records, columns=frame_columns)
    df_shape = df.shape
    if df_shape[0] > 0:
        df.columns = frame_columns
    else:
        return pd.DataFrame
    cursor.close()
    return df


def create_strategy_answer_json(data_df, open_ai_client, base_prompt):
    # output will contain a dictionary with the following structure:
    result_dict = dict()
    message_text = []
    gpt4_sys_msg = "You are a completion service which is generating synthetic data. what you generate can be false. Do not add any conversation like language to the completion."
    user_instructions_prompt = base_prompt + "if the question is asking for a date, just provide a date.\
                    if the question is asking for a yes/no answer, just provide yes or no. \
                    Create an answer for the given question as a best possible guess. You are generating synthetic answer, so the answer can be false. If it is not possible to formulate an answer return an empty string."
    message_text.append({"role": "system", "content": gpt4_sys_msg})
    message_text.append({"role": "user", "content": user_instructions_prompt})

    for row_index, row_item in data_df.iterrows():
        schema_dict = json.loads(row_item['schema'])
        schema_property_dict = schema_dict['properties']
        for key_value in schema_property_dict.keys():
            friendly_name = None
            question_meta_data = schema_property_dict[key_value]
            if question_meta_data["type"] == "string":
                friendly_name = question_meta_data["title"]
            # elif question_meta_data["type"] == "array":
            #     if "title" in question_meta_data.keys():
            #         friendly_name = question_meta_data["title"]
            #     elif "arrayTitle" in question_meta_data.keys():
            #         friendly_name = question_meta_data["arrayTitle"]
                if friendly_name is None:
                    friendly_name = key_value
                
                print(friendly_name)
                message_text.append({"role": "user", "content": friendly_name})
                content = interact_with_gpt4(message_text, open_ai_client)['choices'][0]['message']['content']
                print(content)
                message_text.append({"role": "assistant", "content": content})
                #formatting in html
                system_msg = "You are a html formatter. if the content is a date, an empty string, or a yes/no answer, do not add any html tags.Else add paragraph or ordered or unordered list tags as appropriate. Do not add any extra content."
                html_format_prompt =  content
                final_content = interact_with_chat_application(html_format_prompt, open_ai_client, system_msg)['choices'][0]['message']['content']
                result_dict[key_value] = final_content
                if len(message_text) > 15:
                    message_text.pop(2)
                    message_text.pop(2)
    return result_dict


def create_firm_answer_json(data_df, open_ai_client, base_prompt):
    # output will contain a dictionary with the following structure:
    result_dict = dict()
    for row_index, row_item in data_df.iterrows():
        schema_dict = json.loads(row_item['schema'])
        schema_property_dict = schema_dict['properties']
        for key_value in schema_property_dict.keys():
            friendly_name = None
            question_meta_data = schema_property_dict[key_value]
            if question_meta_data["type"] == "string":
                friendly_name = question_meta_data["title"]
            # elif question_meta_data["type"] == "array":
            #     if "title" in question_meta_data.keys():
            #         friendly_name = question_meta_data["title"]
            #     elif "arrayTitle" in question_meta_data.keys():
            #         friendly_name = question_meta_data["arrayTitle"]
                if friendly_name is None:
                    friendly_name = key_value
                prompt = base_prompt + " For an asset management firm described as above provide possible answers for the questions. \
                    if the question is asking for a date, just provide a date.\
                    if the question is asking for a yes/no answer, just provide yes or no. \
                    If it is not possible to formulate an answer return an empty string. \
                Do not indicate whether you created the answer based on the given information or whether it was created using best possible guess. question: " + friendly_name 
                print(key_value)
                content = interact_with_chat_application(prompt, open_ai_client)['choices'][0]['message']['content']
                #formatting in html
                system_msg = "You are a html formatter. if the content is a date, an empty string, or a yes/no answer, do not add any html tags.Else add paragraph or ordered or unordered list tags as appropriate. Do not add any extra content."
                html_format_prompt =  content
                final_content = interact_with_chat_application(html_format_prompt, open_ai_client, system_msg)['choices'][0]['message']['content']
                result_dict[key_value] = final_content
    return result_dict


def generate_firm_information(client,section_type=None):
    cnxn = azure_sql_connection(client)

    firm_description = "Assette Capital is a global investment management firm with an AUM of $2.5trillion that has been around for 25 years as of 2024/01/25. Assette Capital provides a wide range of investment solutions to a diverse client base. \
    The firm offers a variety of investment strategies across multiple asset classes, including equities, fixed income, and alternative investments. \
    Assette Capital is committed to delivering superior investment performance and exceptional client service."
    
    open_ai_client = get_openai_client_obj()
    
    if section_type != "All":
        section_type_string='FirmInfo'+section_type
        query = "select top(1) qs.SchemaJSON from QualitativeInputStructure qs where qs.name = '" + section_type_string + "'"
        df = execute_data_queries(query, cnxn, frame_columns=['schema'])
        result_dict = create_firm_answer_json(df, open_ai_client, firm_description)
        return result_dict

    elif section_type =="All":
        # Iterate through all section types and generate the firm info
        firm_info_section_types_query = "select id,type from FirmInfoSectionTypes where isactive = 1"
        section_types = execute_data_queries(firm_info_section_types_query, cnxn)

        for row_index, section_data in section_types.iterrows():
            section_name = 'FirmInfo'+section_data['type']
            print(section_name)
            query = "select top(1) qs.SchemaJSON from QualitativeInputStructure qs where qs.name = '" + section_name + "'"
            print(query)

            df = execute_data_queries(query, cnxn, frame_columns=['schema'])
            result_dict = create_firm_answer_json(df, open_ai_client, firm_description)
            out_file = open(section_data['type']+".json", "w")
            json.dump(result_dict, out_file,indent=4)
            out_file.close()

def generate_strategy_information(client, section_type, strategy_name, strategy_description):
    cnxn = azure_sql_connection(client)

    firm_description = "Assette Capital is a global investment management firm with an AUM of $2.5trillion that has been around for 25 years as of 2024/01/25. Assette Capital provides a wide range of investment solutions to a diverse client base. \
    The firm offers a variety of investment strategies across multiple asset classes, including equities, fixed income, and alternative investments. \
    Assette Capital is committed to delivering superior investment performance and exceptional client service."

    open_ai_client = get_openai_client_obj()

    base_prompt = firm_description+ "\n The asset management firm described above manages an investment strategy named, '"+strategy_name+"'. The strategy has the following description: '"+strategy_description+"'."


    if section_type == "All" and strategy_name is not None:
        strategy_info_section_types_query = "select id,type from StrategyInfoSectionTypes where isactive = 1"
        section_types = execute_data_queries(strategy_info_section_types_query, cnxn)

        for row_index, section_data in section_types.iterrows():
            # if section_data['type'] not in ('Product','Teams','Vehicle'):
            if section_data['type'] in ('Investment'):
                section_name = 'StrategyInfo'+section_data['type']
                print(section_name)
                query = "select top(1) qs.SchemaJSON from QualitativeInputStructure qs where qs.name = '" + section_name + "'"
                print(query)

                df = execute_data_queries(query, cnxn, frame_columns=['schema'])
                result_dict = create_strategy_answer_json(df, open_ai_client, base_prompt)
                out_file = open(section_data['type']+".json", "w")
                json.dump(result_dict, out_file,indent=4)
                out_file.close()

    elif section_type !="All" and strategy_name is not None:
        if section_type not in ('Product','Teams','Vehicle'):
            section_name = 'StrategyInfo'+section_type
            print(section_name)
            query = "select top(1) qs.SchemaJSON from QualitativeInputStructure qs where qs.name = '" + section_name + "'"
            print(query)

            df = execute_data_queries(query, cnxn, frame_columns=['schema'])
            result_dict = create_strategy_answer_json(df, open_ai_client, base_prompt)
            out_file = open(section_type+".json", "w")
            json.dump(result_dict, out_file,indent=4)
            out_file.close()
    
    cnxn.close()


if __name__ == '__main__':
    # qualitative_data_embeddings_orchestrator('Strategy', None, strategy_name="All Country World ex-U.S. Value")
    # section_type = 'All'
    # generate_firm_information("BYOD",section_type)

    strategy_name = "US Large Cap Growth Equity Strategy"
    strategy_description = "US Large Cap Growth Equity Strategy manages an AUM of USD 100 billion. The strategy has been around for 15 years. The strategy focuses on a bottom up approach to investing and invests in growth securities. Strategy has some ESG considerations when investing."
    generate_strategy_information("BYOD", "Administrative", strategy_name, strategy_description)
    # print(result)

