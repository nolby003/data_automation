import os, env
import openai
API_KEY = os.env.get(API_KEY)
ai_model = 'gpt-3.5-turbo'
prompt = 'The task is to give me the SQL data type for the given field name.'
conversation = [
    {
        "role": "system",
        "content": prompt
    }
]
def pass_field_to_ai(field):
    conversation.append(
        {
            "role": "user",
            "content": field
        }
    )
    response = openai.ChatCompletion.create(
        model=ai_model,
        messages=conversation
    )
    message = response["choices"][0]["message"]["content"]
    conversation.append(
        {
            "role": "assistant",
            "content": message
        }
    )
    return message
