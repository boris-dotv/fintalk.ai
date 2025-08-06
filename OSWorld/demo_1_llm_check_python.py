import argparse
import datetime
import json
import logging
import os
import sys
from tqdm import tqdm
import subprocess
import requests
from desktop_env.desktop_env_llm import DesktopEnv

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("----------------------------------------------------")
print("Starting execution of demo_llm_check_python.py script...")
print("----------------------------------------------------")

# --- Example Task Configuration ---
# This task defines the goal and how success is evaluated.
example_task = {
    "id": "94d95f96-9699-4208-98ba-3c3119edf9c2",
    "instruction": "Check if python3 is installed.",
    "config": [],
    "evaluator": {
        "func": "check_include_exclude",
        "result": {
            "type": "vm_command_line",
            "command": "which python3"
        },
        "expected": {
            "type": "rule",
            "rules": {
                "include": ["/usr/local/python3.10.17/bin/python3"], # Adjust this path if your container's path differs
                "exclude": ["not found"]
            }
        }
    }
}

# --- LLM Agent Function ---
def get_llm_action(instruction: str, model_endpoint: str) -> dict:
    """
    Calls the LLM to get the next action based on the instruction.
    """
    prompt = f"""
You are an intelligent agent operating a Linux desktop inside a Docker container.
Your task is to generate the most direct and concise shell command based on the user's instruction.

Here is an example:
---
User instruction: "Check if git is installed."

Your JSON output:
```json
{{
  "action_type": "execute_command",
  "command": "which git"
}}
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Python
IGNORE_WHEN_COPYING_END

Now, generate your output based on the new instruction below.

User instruction: "{instruction}"

Your JSON output:
"""

    headers = {"Content-Type": "application/json"}
    payload = {
        "model": "/osworld/Qwen2.5-VL-7B-Instruct",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that generates shell commands based on instructions."},
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
        "max_tokens": 256
    }

    try:
        print("\n--- Sending request to agent_a ---")
        print(f"Instruction: {instruction}")
        response = requests.post(model_endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()

        response_json = response.json()
        llm_output_content = response_json['choices'][0]['message']['content']

        print(f"--- Raw Response Text Content from Qwen2.5-VL-7B-Instruct ---\n{llm_output_content}\n------------------------")

        json_str_match = llm_output_content[llm_output_content.find('{'):llm_output_content.rfind('}')+1]
        action_json = json.loads(json_str_match)

        if "action_type" in action_json and "command" in action_json:
            return action_json
        else:
            print("ERROR: Qwen2.5-VL-7B-Instruct's response is not in the expected JSON format.")
            return {"action_type": "FAIL"}

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Could not connect to LLM endpoint {model_endpoint}. Error: {e}")
        return {"action_type": "FAIL"}
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"ERROR: Could not parse LLM response or find content within it. Error: {e}")
        return {"action_type": "FAIL"}

if __name__ == "__main__":
    CONTAINER_NAME = "agent_b"
    LLM_ENDPOINT = "http://localhost:8000/v1/chat/completions"

    try:
        subprocess.run(['docker', '--version'], check=True, capture_output=True)
        logger.info("Docker is installed and accessible.")
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        logger.error(f"Docker check failed. Please ensure Docker is installed and running: {e}")
        sys.exit(1)

    try:
        result = subprocess.run(['docker', 'inspect', '-f', '{{.State.Running}}', CONTAINER_NAME],
                                capture_output=True, text=True, check=True, timeout=5, errors='ignore')
        if result.stdout.strip() != 'true':
            raise RuntimeError(f"Container '{CONTAINER_NAME}' is not running.")
        logger.info(f"Docker container '{CONTAINER_NAME}' is running.")
    except (subprocess.CalledProcessError, RuntimeError) as e:
        logger.error(f"Failed to verify container '{CONTAINER_NAME}'. Please ensure it is started. Error: {e}")
        sys.exit(1)

    # --- Main Program Execution ---
    try:
        print(f"\nInitializing DesktopEnv with Docker provider and Docker container '{CONTAINER_NAME}'...")
        env = DesktopEnv(
            provider_name="docker",
            os_type="Ubuntu",
            action_space="computer_13",
            path_to_vm=CONTAINER_NAME,
            cache_dir="./docker_cache"
        )
        print("DesktopEnv initialized successfully.")

        print("\nResetting environment with the example task...")
        print("\n--- Checking example_task configuration ---")
        print("Instruction:", example_task.get("instruction"))
        print("Evaluator:", json.dumps(example_task.get("evaluator"), indent=2))
        print("------------------------------------------\n")
        obs = env.reset(task_config=example_task)
        print("Environment reset complete.")

        print("\n--- Using LLM to decide the next action ---")
        current_instruction = obs.get("instruction")
        action_from_llm = get_llm_action(current_instruction, LLM_ENDPOINT)
        print(f"\n--- LLM's Decided Action: {action_from_llm} ---")

        # Initialize variables for the step
        done = False
        info = {}
        reward = 0

        # Execute the action from the LLM
        if action_from_llm and action_from_llm.get("action_type") != "FAIL":
            # reward and done are now computed immediately by env.step
            obs, reward, done, info = env.step(action_from_llm)
            print(f"Step executed. Reward: {reward}, Done: {done}, Info: {info}")
        else:
            print("LLM failed to provide a valid action. Terminating task.")
            done = True
            info = {"fail": True}

        # --- MODIFIED EVALUATION LOGIC ---
        # Since reward and done status are computed in step, we just report the final result
        print("\nReporting final task results...")
        if info.get("success"):
            print(f"Task successful! Final score: {reward}")
        elif info.get("fail"):
            print("Task terminated due to a failed instruction. Final score: 0.0")
        elif not done:
            # If the task is not done within one step (for more complex tasks), call evaluate
            print("Task not completed after single step, performing final evaluation...")
            score = env.evaluate()
            print(f"Evaluation score: {score}")
        else:
            # If done=True but not due to success, it means it reached the end but wasn't successful
            print("Task ended but not explicitly successful, final score: 0.0")

        print("\nClosing environment...")
        env.close()
        print("Environment closed.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during execution: {e}")
        import traceback
        traceback.print_exc()

    print("\n----------------------------------------------------")
    print("demo_llm_check_python.py script execution finished.")
    print("----------------------------------------------------")