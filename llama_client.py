import openai

# Read Open AI key from text file
with open("secret_keys/openai_key.txt", "r") as f:
    api_key = f.read().strip()

openai.api_key = api_key

class LlamaClient:
    def __init__(self, model_name="gpt-4", temperature=0.0):
        self.model_name = model_name
        self.temperature = temperature

    def chat(self, conversation):
        """
        conversation: list of {"role": "user" / "assistant", "content": "..."}

        Returns: {"text": ...}
        """
        # Ensure the conversation is in the correct format expected by the API
        messages = [{"role": entry['role'], "content": entry['content']} for entry in conversation]

        try:
            # Use the correct method for OpenAI chat models
            response = openai.ChatCompletion.create(
                model=self.model_name,        # Set the model (e.g., "gpt-4" or "gpt-3.5-turbo")
                messages=messages,             # Pass the entire conversation history
                temperature=self.temperature,   # Control randomness of responses
                max_tokens=150                 # Optional: Set the maximum tokens for the response
            )
            # Extract the response text from the API response
            text = response['choices'][0]['message']['content']
            return {"text": text}

        except Exception as e:
            return {"error": str(e)}