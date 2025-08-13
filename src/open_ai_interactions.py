# open_ai_interactions.py

import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

def get_openai_client_obj():
    """
    Returns an OpenAI API client object.
    """
    return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def interact_with_gpt4(messages, openai_client, model="gpt-4.1", temperature=0.2, max_tokens=256):
    """
    Calls OpenAI's GPT-4 model with the given messages.
    
    Args:
        messages (list): Conversation in OpenAI chat format.
        openai_client: OpenAI API client instance.
        model (str): Model to use.
        temperature (float): Sampling temperature.
        max_tokens (int): Maximum number of tokens to generate.
    
    Returns:
        dict: OpenAI API response.
    """
    response = openai_client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens
    )
    return response

def interact_with_chat_application(prompt, openai_client, system_message=None, model="gpt-4.1", temperature=0.2, max_tokens=256):
    """
    Calls OpenAI GPT model with a single prompt and optional system message.

    Args:
        prompt (str): The user prompt.
        openai_client: OpenAI API client instance.
        system_message (str): Optional system prompt.
        model (str): Model to use.
        temperature (float): Sampling temperature.
        max_tokens (int): Maximum tokens to generate.

    Returns:
        dict: OpenAI API response.
    """
    messages = []
    if system_message:
        messages.append({"role": "system", "content": system_message})
    messages.append({"role": "user", "content": prompt})

    return interact_with_gpt4(messages, openai_client, model=model, temperature=temperature, max_tokens=max_tokens)
