import json
import logging
import os
import sys
import subprocess
import requests
import base64

from desktop_env.desktop_env_master import DesktopEnv 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) 


def ask_tool_agent(question: str, worker_endpoint: str) -> str:
    """
    Calls agent_b (worker_agent) for a plain text question.
    """
    print(f"--- master_agent Action: Requesting agent_b (worker_agent, at {worker_endpoint}) to answer a text question: '{question}' ---")
    
    headers = {"Content-Type": "application/json"}
    payload = {
        "model": "/osworld/Qwen2.5-VL-7B-Instruct",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": [{"type": "text", "text": question}]}
        ]
    }

    try:
        response = requests.post(worker_endpoint, headers=headers, json=payload, timeout=300)
        response.raise_for_status()
        return json.dumps(response.json())
    except requests.exceptions.RequestException as e:
        return f"Command failed: Could not connect to agent_b (worker_agent). Error: {e}"
    except Exception as e:
        return f"Command failed: An unexpected error occurred while calling agent_b: {e}"

def see_with_tool_agent(image_path: str, question: str, worker_endpoint: str) -> str:
    """
    Calls agent_b (worker_agent) for a multimodal question involving a local image.
    """
    print(f"--- master_agent Action: Requesting agent_b (worker_agent, at {worker_endpoint}) to view '{image_path}' and answer '{question}' ---")

    if not os.path.exists(image_path):
        return f"Command failed: Image file not found at path {image_path}"

    try:
        with open(image_path, "rb") as image_file:
            image_base64 = base64.b64encode(image_file.read()).decode('utf-8')
        image_format = os.path.splitext(image_path)[1].lstrip('.').lower()
        if image_format == 'jpg': image_format = 'jpeg'
        image_data_url = f"data:image/{image_format};base64,{image_base64}"
    except Exception as e:
        return f"Command failed: Could not read or encode image file. Error: {e}"

    headers = {"Content-Type": "application/json"}
    payload = {
        "model": "/osworld/Qwen2.5-VL-7B-Instruct",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that can describe images."},
            {"role": "user", "content": [{"type": "image_url", "image_url": {"url": image_data_url}}, {"type": "text", "text": question}]}
        ]
    }
    
    try:
        response = requests.post(worker_endpoint, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        return json.dumps(response.json())
    except requests.exceptions.RequestException as e:
        return f"Command failed: Could not connect to agent_b (worker_agent). Error: {e}"
    except Exception as e:
        return f"Command failed: An unexpected error occurred while calling agent_b: {e}"

def read_code_with_tool_agent(file_path: str, question: str, worker_endpoint: str) -> str:
    """
    Asks agent_b (worker_agent) to read a code file at the specified path on the host and answer a question.
    """
    print(f"--- master_agent Action: Requesting agent_b (worker_agent) to read local code '{file_path}' on the host and answer '{question}' ---")

    # 1. Read file content directly on the host
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            file_content = f.read()
    except FileNotFoundError:
        return f"Command failed: File not found on host: {file_path}"
    except Exception as e:
        return f"Command failed: Error reading file '{file_path}' on host: {e}"

    # 2. Send the file content to agent_b's LLM for analysis
    code_analysis_prompt = f"""
You are a professional code reviewer. Please carefully read and analyze the code below, then answer the user's question.

Code content:
```python
{file_content}
```

User's question: "{question}"

Your analysis:
"""
    
    headers = {"Content-Type": "application/json"}
    payload = {
        "model": "/osworld/Qwen2.5-VL-7B-Instruct",
        "messages": [
            {"role": "system", "content": "You are a professional code analyst."},
            {"role": "user", "content": [{"type": "text", "text": code_analysis_prompt}]}
        ]
    }
    
    try:
        response = requests.post(worker_endpoint, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        return json.dumps(response.json())
    except requests.exceptions.RequestException as e:
        return f"Command failed: Could not connect to agent_b (worker_agent). Error: {e}"
    except Exception as e:
        return f"Command failed: An unexpected error occurred while calling agent_b for code analysis: {e}"


# --- master_agent (agent_a) Decision Function ---
def get_llm_action(
    conversation_history: list, 
    decision_endpoint: str, 
    last_command_output: str = None
) -> dict:
    """
    Calls agent_a (on port 8000) to decide the next action. This is agent_a's core decision logic.
    """
    system_prompt = """
You are the master_agent (Controller Agent). Your role is to analyze the user's request and then decide which tool to use to interact with agent_b (worker_agent).
agent_b has four capabilities, which you can invoke by selecting the corresponding tool:
1.  **AI Assistant**: For answering all general plain text questions.
2.  **Vision Assistant**: For analyzing the content of local image files that you provide the path for.
3.  **Code Interpreter**: For reading and analyzing the content of local code files that you provide the path for (e.g., .py, .js, .sh, .json, .md, .txt).
4.  **Container Environment**: For running general shell commands within agent_b's Docker space.

**Your Golden Rule: You are ONLY responsible for making decisions and selecting tools. You do NOT directly execute commands or answer questions. All tasks are delegated to agent_b.**

**Your Task Flow:**
1.  Analyze the user's request.
2.  If the user asks a general text question, select the "AI Assistant" tool.
3.  If the user asks about the content of a specific file path ending in .png, .jpg, .jpeg, or .webp, select the "Vision Assistant" tool.
4.  If the user asks to describe, analyze, or explain the content of a file path ending in common code or text extensions like .py, .js, .sh, .json, .md, .txt, etc., select the "Code Interpreter" tool.
5.  If the user requests to execute a general shell command (e.g., `ls`, `pwd`, `echo`), select the "Container Environment" tool.
6.  Output your chosen tool and its required parameters in the strict JSON format specified below.

**Tool Specifications (Output Format):**

*   To use **AI Assistant** (calls agent_b for text Q&A):
    `{"action_type": "ask_tool_agent", "question": "The user's text question"}`

*   To use **Vision Assistant** (calls agent_b for visual Q&A):
    `{"action_type": "see_with_tool_agent", "image_path": "/path/to/the/image.png", "question": "The user's question about the image"}`
    
*   To use **Code Interpreter** (calls agent_b for code analysis):
    `{"action_type": "read_code_with_tool_agent", "file_path": "/path/to/the/code.py", "question": "The user's question about the code"}`

*   To use **Container Environment** (executes commands inside agent_b):
    `{"action_type": "execute_container_command", "command": "The shell command to run"}`
    
*   When the task is complete:
    `{"action_type": "DONE"}`

**Conversation History:**
"""

    
    history_str = "\n".join([f"{msg['role']}: {msg['content']}" for msg in conversation_history])
    if last_command_output:
        history_str += f"\nworker_agent_output: {last_command_output}"

    prompt = f"{system_prompt}\n{history_str}\n\nmaster_agent's next action (JSON):"
    
    headers = {"Content-Type": "application/json"}
    payload = {
        "model": "/osworld/Qwen2.5-VL-7B-Instruct",
        "messages": [
            {"role": "system", "content": "You are an agent that selects tools and parameters in the specified JSON format based on your role and history."},
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
        "max_tokens": 8192,
        "temperature": 0.0
    }

    try:
        print(f"\n--- Requesting master_agent ({decision_endpoint}) for a plan ---")
        response = requests.post(decision_endpoint, headers=headers, json=payload, timeout=45)
        response.raise_for_status()
        response_json = response.json()
        llm_output_content = response_json['choices'][0]['message']['content']
        print(f"--- Raw Response from master_agent LLM ---\n{llm_output_content}\n-----------------------------")

        json_str_match = llm_output_content[llm_output_content.find('{'):llm_output_content.rfind('}')+1]
        if not json_str_match:
            if "```json" in llm_output_content:
                start = llm_output_content.find("```json") + len("```json")
                end = llm_output_content.rfind("```")
                json_str_match = llm_output_content[start:end].strip()
            else:
                 raise json.JSONDecodeError("JSON object not found in response", llm_output_content, 0)
        
        action_json = json.loads(json_str_match)
        
        if "action_type" in action_json:
            return action_json
        else:
            print("ERROR: master_agent LLM's response is not in the expected JSON format.")
            return {"action_type": "FAIL"}

    except Exception as e:
        print(f"ERROR: An error occurred while communicating with master_agent LLM: {e}")
        return {"action_type": "FAIL"}


if __name__ == "__main__":
    # --- Configuration ---
    # agent_b is the controlled container and acts as the worker_agent
    WORKER_CONTAINER_NAME = "agent_b" 
    # Host port for the master_agent (i.e., agent_a)
    DECISION_AGENT_ENDPOINT = "http://localhost:8000/v1/chat/completions"
    # Host port provided by agent_b
    WORKER_AGENT_ENDPOINT = "http://localhost:8001/v1/chat/completions"
    
    # --- Pre-flight Checks ---
    logger.info("Performing pre-flight checks...")
    try:
        # Check if agent_b container is running
        subprocess.run(['docker', 'inspect', '-f', '{{.State.Running}}', WORKER_CONTAINER_NAME], 
                       capture_output=True, text=True, check=True, timeout=5, errors='ignore')
        logger.info(f"Worker container '{WORKER_CONTAINER_NAME}' (agent_b) is running.")
        # Check if agent_b's tool endpoint is accessible
        requests.get(WORKER_AGENT_ENDPOINT, timeout=3)
        logger.info(f"agent_b (worker_agent) tool endpoint at {WORKER_AGENT_ENDPOINT} appears to be accessible.")
    except Exception as e:
        logger.error(f"Pre-flight check failed. Please ensure container '{WORKER_CONTAINER_NAME}' is running and its LLM service is available on port 8001. Error: {e}")
        sys.exit(1)

    env = None
    try:
        print(f"\nInitializing DesktopEnv to interact with Worker container '{WORKER_CONTAINER_NAME}'...")
        env = DesktopEnv(
            provider_name="docker",
            os_type="Ubuntu",
            path_to_vm=WORKER_CONTAINER_NAME,
            cache_dir="./docker_cache"
        )
        print("DesktopEnv initialized successfully.")

        # --- Main Interaction Loop ---
        conversation_history = []
        print("\n=======================================================")
        print(" Chatting with master_agent (agent_a)")
        print(" (It will command agent_b to complete tasks)")
        print("=======================================================")

        while True:
            user_input = input("\nYou (to agent_a): ")
            if user_input.lower() in ['exit', 'quit']:
                break

            conversation_history.append({"role": "user", "content": user_input})
            last_command_output = conversation_history[-1]['content'] if len(conversation_history) > 1 and conversation_history[-1]['role'] == 'assistant' else None
            
            action = get_llm_action(conversation_history, DECISION_AGENT_ENDPOINT, last_command_output)
            print(f"\n>>> master_agent's Decided Action: {action}")

            if not action or action.get("action_type") in ["FAIL", "DONE"]:
                print("master_agent decided to end the conversation or has failed.")
                break
            
            action_type = action.get("action_type")
            tool_response = ""

            if action_type == "ask_tool_agent":
                question = action.get("question")
                tool_response = ask_tool_agent(question, WORKER_AGENT_ENDPOINT) if question else "ERROR: 'ask_tool_agent' action missing 'question' parameter."
            
            elif action_type == "see_with_tool_agent":
                image_path = action.get("image_path")
                question = action.get("question")
                tool_response = see_with_tool_agent(image_path, question, WORKER_AGENT_ENDPOINT) if image_path and question else "ERROR: 'see_with_tool_agent' action missing 'image_path' or 'question' parameter."

            elif action_type == "read_code_with_tool_agent":
                file_path = action.get("file_path")
                question = action.get("question")
                if file_path and question:
                    tool_response = read_code_with_tool_agent(file_path, question, WORKER_AGENT_ENDPOINT)
                else:
                    tool_response = "ERROR: 'read_code_with_tool_agent' action missing 'file_path' or 'question' parameter."

            elif action_type == "execute_container_command":
                command = action.get("command")
                if command:
                    container_action = {"action_type": "execute_command", "command": command}
                    obs, _, _, _ = env.step(container_action)
                    tool_response = obs.get("command_output", "Container command produced no output.")
                else:
                    tool_response = "ERROR: 'execute_container_command' action missing 'command' parameter."

            else:
                tool_response = f"ERROR: master_agent generated an unknown action type '{action_type}'."
            
            print(f"\nResult from agent_b (worker_agent) execution:\n---\n{tool_response}\n---")

            assistant_message = ""
            try:
                if tool_response.startswith("Command failed") or tool_response.startswith("ERROR:"):
                    assistant_message = tool_response
                else:
                    parsed_tool_response = json.loads(tool_response)
                    if parsed_tool_response.get("object") == "error":
                        assistant_message = f"agent_b returned an error: {parsed_tool_response.get('message')}"
                    else:
                        assistant_message = parsed_tool_response['choices'][0]['message']['content']
            except (json.JSONDecodeError, KeyError, IndexError, TypeError):
                assistant_message = tool_response

            conversation_history.append({"role": "assistant", "content": assistant_message})
            print(f"\nmaster_agent (relaying agent_b's reply): {assistant_message}")

    except Exception as e:
        logger.error(f"An unexpected error occurred during execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if env:
            print("\nShutting down environment...")
            env.close()
        print("Session ended. Goodbye!")
