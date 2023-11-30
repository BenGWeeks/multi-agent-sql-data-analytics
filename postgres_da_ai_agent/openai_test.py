import openai
import os
#import dotenv

#dotenv.load_dotenv()

# Check environment variables
#assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not found in .env file"

# Get environment variables
#OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Set your OpenAI API key
#openai.api_key = OPENAI_API_KEY
openai.api_key = "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"

# List all available models
model_lst = openai.Model.list()

# Print the IDs of each available model
for model in model_lst['data']:
    print(model['id'])