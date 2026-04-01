"""
Conversational AI Bot using DialoGPT (pre-trained, no training needed)
Run this script to start a chat interface in your browser.
"""

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
import gradio as gr

# ----------------------------------------------------------------------
# 1. Load the pre-trained model and tokenizer
# ----------------------------------------------------------------------
MODEL_NAME = "microsoft/DialoGPT-small"   # Change to "medium" or "large" if you have more GPU memory
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForCausalLM.from_pretrained(MODEL_NAME)

# ----------------------------------------------------------------------
# 2. Conversation memory – keep only the last N turns to avoid context overflow
# ----------------------------------------------------------------------
MAX_HISTORY_TURNS = 5   # remember the last 5 exchanges (user + bot)
conversation_history = []   # list of (user_input_ids, bot_response_ids)

def get_response(user_input: str) -> str:
    """
    Generate a response from the model given the current user input and conversation history.
    """
    global conversation_history

    # Encode the user input with an end-of-sequence token
    new_user_ids = tokenizer.encode(user_input + tokenizer.eos_token, return_tensors='pt')

    # Build the full model input: all previous turns + the new user input
    input_ids = new_user_ids
    for user_ids, bot_ids in conversation_history:
        # Concatenate: previous user, previous bot, and current input
        input_ids = torch.cat([user_ids, bot_ids, input_ids], dim=-1)

    # Generate a response
    with torch.no_grad():
        chat_history_ids = model.generate(
            input_ids,
            max_length=1000,
            pad_token_id=tokenizer.eos_token_id,
            do_sample=True,
            top_k=50,
            top_p=0.95,
            temperature=0.7,
        )

    # Decode only the newly generated part (skip the input part)
    response = tokenizer.decode(
        chat_history_ids[:, input_ids.shape[-1]:][0],
        skip_special_tokens=True
    )
    new_bot_ids = chat_history_ids[:, input_ids.shape[-1]:]

    # Store the current turn in history
    conversation_history.append((new_user_ids, new_bot_ids))

    # Trim history if it exceeds the maximum number of turns
    if len(conversation_history) > MAX_HISTORY_TURNS:
        conversation_history.pop(0)

    return response

# ----------------------------------------------------------------------
# 3. Gradio interface
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 3. Gradio interface
# ----------------------------------------------------------------------
def chat_interface(message, history):
    """
    Function called by Gradio each time the user sends a message.
    'history' is a list of [user, bot] pairs (Gradio keeps it automatically).
    """
    bot_reply = get_response(message)
    return bot_reply

# Create the chat interface – remove 'theme' if it's not supported
demo = gr.ChatInterface(
    fn=chat_interface,
    title="🤖 My LLM Chatbot",
    description="A conversational bot using DialoGPT (pre-trained). It doesn't need any training data – just talk to it!"
    # theme="soft"   # <-- removed because it caused TypeError
)

# ----------------------------------------------------------------------
# 4. Launch the app
# ----------------------------------------------------------------------
if __name__ == "__main__":
    demo.launch(share=False)   # Set share=True to get a public link