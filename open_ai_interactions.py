from openai import AzureOpenAI
import time
import json
import os
import logging
import dotenv
dotenv.load_dotenv("local_config.env")

def get_openai_client_obj():
    client = AzureOpenAI(
        # This is the default and can be omitted
        api_key=os.environ["OpenAI_Key"],
        api_version=os.environ["openai_api_version"],
        azure_endpoint=os.environ["OpenAI_EndPoint"]
    )
    return client

def interact_with_chat_application(prompt, client, system_msg = "You are a completion service. Do not add any conversation like language to the completion."):
    deployment_name = os.getenv("chat_deployment_name")
    if deployment_name is None:
        logging.error("Environment variable 'chat_deployment_name' is not set.")
        return []

    attempt_num = 1
    is_attempt_success = False
    response = None
    while attempt_num <= 3 and not is_attempt_success:
        try:
            response = json.loads(client.chat.completions.create(model=deployment_name, messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt}], temperature=0.7,
                                                                 max_tokens=2000,
                                                                 top_p=0.95,
                                                                 frequency_penalty=0,
                                                                 presence_penalty=0,
                                                                 stop=None).model_dump_json())
            is_attempt_success = True
        except Exception as err:
            if attempt_num <= 3:
                logging.error(f"Error in interacting with chat application: {err}")
                logging.warn("sleeping the process and restarting")
                time.sleep(3 ** attempt_num)
                attempt_num += 1

    if not is_attempt_success:
        raise RuntimeError("Embedding generation failed for: ", prompt)

    return response

def interact_with_gpt4(message_text, client, use_mini_model=False):

    deployment_name = None
    if use_mini_model:
        deployment_name = os.getenv("chat_deployment_name_mini")
    else:
        deployment_name = os.getenv("chat_deployment_name")

    if deployment_name is None:
        logging.error("Environment variable 'chat_deployment_name' is not set.")
        return []

    attempt_num = 1
    is_attempt_success = False
    response = None
    while attempt_num <= 3 and not is_attempt_success:
        try:
            response = json.loads(client.chat.completions.create(model=deployment_name, messages=message_text, 
                                                                 temperature=0.7,
                                                                 max_tokens=2000,
                                                                 top_p=0.95,
                                                                 frequency_penalty=0,
                                                                 presence_penalty=0,
                                                                 stop=None).model_dump_json())
            is_attempt_success = True
        except Exception as err:
            if attempt_num <= 3:
                logging.error(f"Error in interacting with chat application: {err}")
                logging.warning("sleeping the process and restarting")
                time.sleep(3 ** attempt_num)
                attempt_num += 1

    if not is_attempt_success:
        raise RuntimeError("generation failed for: ", message_text)

    return response