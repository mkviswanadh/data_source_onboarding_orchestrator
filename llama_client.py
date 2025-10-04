### `llama_client.py`


import openai

openai.api_key = "YOUR_OPENAI_API_KEY"

class LlamaClient:
    def __init__(self, model_name="llama3.8", temperature=0.0):
        self.model_name = model_name
        self.temperature = temperature

    def chat(self, conversation):
        """
        conversation: list of {"role": "user" / "assistant", "content": "..."}
        Returns: {"text": ...}
        """
        resp = openai.ChatCompletion.create(
            model="gpt-4",
            messages=conversation,
            temperature=self.temperature
        )
        text = resp.choices[0].message.content
        return {"text": text}
