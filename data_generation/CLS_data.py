# Based on our previous analysis of the Orchestrator's needs, the essential classes we identified are:
# 1.  **`DATA_RETRIEVAL`**: A standard query for specific information in the database.
# 2.  **`COMPARISON`**: A query that requires comparing two or more entities.
# 3.  **`AGGREGATION`**: A query that needs a calculation like `COUNT`, `AVG`, or `SUM`.
# 4.  **`DATA_RETRIEVAL_AMBIGUOUS`**: A query that asks for data but is missing a critical entity (like a company name), requiring the Orchestrator to ask a clarifying question.
# 5.  **`GENERAL_KNOWLEDGE`**: A query not related to the internal database, which the Orchestrator should answer itself.
# 6.  **`COMPOUND_REQUEST`**: A complex query mixing multiple intents (e.g., data retrieval and general knowledge).

import os
import json
import re
import time
import random
import numpy as np
from openai import OpenAI, APIConnectionError, RateLimitError
from typing import List, Dict, Any
from loguru import logger

# --- 1. Configuration ---

# This is a public, free-credit API key provided for demonstration and testing purposes.
# It is intentionally exposed and has usage limits. For production, please use your own key.
DEEPSEEK_API_KEY = "sk-c667ae5858c34180808b7752ec6b36fd"

try:
    client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")
    logger.info("OpenAI client initialized for DeepSeek API.")
except Exception as e:
    logger.error(f"Failed to initialize OpenAI client: {e}")
    exit()

TARGET_DATASET_SIZE = 5000
SFT_OUTPUT_FILE = "sft_classification_dataset.jsonl"

# --- 2. Embedding Model Logic (Prepared and Commented Out) ---

# To enable semantic deduplication, you will need to:
# 1. Download the Qwen/Qwen3-Embedding-8B model to the specified path.
# 2. Install the required libraries: pip install sentence-transformers transformers accelerate qdrant-client
# 3. Uncomment all blocks marked with "[Embedding Logic]".

# [Embedding Logic]
# class EmbeddingModel:
#     """
#     A wrapper for the Qwen/Qwen3-Embedding-8B model using sentence-transformers.
#     """
#     def __init__(self, model_id: str = "Qwen/Qwen3-Embedding-8B"):
#         self.model_id = model_id
#         self.model = None
#         try:
#             from sentence_transformers import SentenceTransformer
#             self.model = SentenceTransformer(self.model_id, device_map="auto", trust_remote_code=True)
#             self.dim = self.model.get_sentence_embedding_dimension()
#             logger.success(f"Successfully loaded embedding model '{self.model_id}' with dimension {self.dim}.")
#         except Exception as e:
#             logger.error(f"Failed to load embedding model. Error: {e}")
#             raise
#
#     def encode(self, texts: List[str], is_query: bool = True) -> np.ndarray:
#         if not self.model:
#             raise RuntimeError("Embedding model is not loaded.")
#         prompt = "query" if is_query else None
#         return self.model.encode(texts, prompt_name=prompt)

# [Embedding Logic]
# from qdrant_client import QdrantClient, models
# qdrant_client = QdrantClient(":memory:")
# QDRANT_COLLECTION_NAME = "sft_cls_queries_collection"
# SIMILARITY_THRESHOLD = 0.95

# [Embedding Logic]
# def setup_vector_db(embedding_dim):
#     """Initializes the Qdrant vector database collection."""
#     try:
#         qdrant_client.recreate_collection(
#             collection_name=QDRANT_COLLECTION_NAME,
#             vectors_config=models.VectorParams(size=embedding_dim, distance=models.Distance.COSINE),
#         )
#         logger.success(f"Successfully created Qdrant collection '{QDRANT_COLLECTION_NAME}'.")
#     except Exception as e:
#         logger.error(f"Failed to create Qdrant collection: {e}")
#         raise

# Using simple text-based deduplication as the default
existing_queries_set = set()


# --- 3. Dynamic Prompt Engineering for Classification Data ---

HIGH_QUALITY_CLS_EXAMPLES_POOL = [
    {
        "user_query": "Can you get me the website for Ramp?",
        "intent_class": "DATA_RETRIEVAL"
    },
    {
        "user_query": "Which company is larger in terms of employee count, Arival or Lendo?",
        "intent_class": "COMPARISON"
    },
    {
        "user_query": "What is the total number of shareholders across all companies tagged as 'Finance'?",
        "intent_class": "AGGREGATION"
    },
    {
        "user_query": "Tell me the name of the CEO.",
        "intent_class": "DATA_RETRIEVAL_AMBIGUOUS"
    },
    {
        "user_query": "What is venture capital?",
        "intent_class": "GENERAL_KNOWLEDGE"
    },
    {
        "user_query": "Find the tech summary for Airstar Bank and also explain what 'due diligence' means.",
        "intent_class": "COMPOUND_REQUEST"
    },
    {
        "user_query": "show me all the managers in the 'Director' department for ZA Bank",
        "intent_class": "DATA_RETRIEVAL"
    }
]

DIVERSITY_INSTRUCTIONS_POOL = [
    "Generate a vague query that is missing the main company entity, making it ambiguous.",
    "Create a query that mixes a database query with a general financial definition.",
    "Formulate a question that asks for a calculated total or average across the entire dataset.",
    "Write a very simple, direct question asking for a specific piece of data for a named company.",
    "Generate a question that is purely definitional and has no connection to the companies in the database.",
    "Create a query that requires comparing two different metrics for the same company.",
    "Write a conversational query with filler words and politeness."
]

def generate_diverse_cls_prompt(recent_examples: List[Dict[str, str]]) -> str:
    """
    Constructs a dynamically randomized prompt with negative feedback
    to guide the LLM towards generating diverse classification training data.
    """
    few_shot_examples = random.sample(HIGH_QUALITY_CLS_EXAMPLES_POOL, 4)
    examples_str = "\n---\n".join([json.dumps(ex, indent=2) for ex in few_shot_examples])
    
    diversity_instruction = random.choice(DIVERSITY_INSTRUCTIONS_POOL)

    negative_feedback_str = ""
    if recent_examples:
        negative_feedback_str = "To ensure diversity, AVOID generating queries that are semantically similar to these recently generated ones:\n"
        for ex in recent_examples:
            negative_feedback_str += f"- \"{ex['user_query']}\"\n"
        negative_feedback_str += "\n"

    return f"""
You are an expert data scientist specializing in intent classification. Your task is to generate pairs of realistic user queries and their corresponding intent class. This data will be used to train a model that routes tasks for a financial AI assistant.

Here are the possible intent classes you must assign:
- `DATA_RETRIEVAL`: A standard, specific request for information from the database (e.g., "What is the employee size of company X?").
- `COMPARISON`: The user wants to compare two or more entities or metrics (e.g., "Which company has more employees, X or Y?").
- `AGGREGATION`: The user wants a calculated value across multiple records, like a count, average, or sum (e.g., "How many companies are in the 'large' size category?").
- `DATA_RETRIEVAL_AMBIGUOUS`: The user is asking for data but did not specify a crucial entity like the company name (e.g., "Who is the chairman?").
- `GENERAL_KNOWLEDGE`: The query is a general knowledge question not related to the internal database (e.g., "What is a Series B funding round?").
- `COMPOUND_REQUEST`: The query contains multiple distinct tasks or intents (e.g., "List investors for company X and explain what an angel investor is.").

CRITICAL INSTRUCTIONS:
1.  **Generate Realistic Input**: The 'user_query' should be conversational, and may contain filler words, politeness, or irrelevant context.
2.  **Assign Correct Class**: The 'intent_class' must be one of the six classes defined above.
3.  **Follow the Diversity Hint**: Pay close attention to the special instruction below to guide your creativity.
4.  **AVOID REPETITION**: {negative_feedback_str if negative_feedback_str else "Do not repeat patterns from the positive examples."}
5.  **Output Format**: Your entire output must be a single, valid JSON object with a "user_query" key and an "intent_class" key.

---
Here are some POSITIVE examples to inspire you. Do NOT copy their patterns directly.
{examples_str}
---

**SPECIAL DIVERSITY HINT FOR THIS GENERATION**: {diversity_instruction}

Now, following all instructions and the special diversity hint, generate one new, realistic, and unique user query and assign it the correct intent class.
"""

# --- 4. Main Generation Pipeline ---

def main():
    """Main function to orchestrate the SFT dataset generation pipeline for CLS."""
    logger.info("--- Starting CLS Dataset Generation Pipeline (v3 with Dynamic Prompts) ---")
    
    embedding_model_instance = None
    # [Embedding Logic]
    # try:
    #     embedding_model_instance = EmbeddingModel()
    #     setup_vector_db(embedding_model_instance.dim)
    # except Exception:
    #     logger.warning("Could not initialize embedding model. Falling back to text-based deduplication.")
    #     embedding_model_instance = None

    sft_dataset: List[Dict[str, Any]] = []

    if os.path.exists(SFT_OUTPUT_FILE):
        logger.info(f"Loading existing dataset from {SFT_OUTPUT_FILE} to resume.")
        with open(SFT_OUTPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    sft_dataset.append(data)
                    existing_queries_set.add(data['user_query'].lower().strip())
                except json.JSONDecodeError:
                    continue
        logger.info(f"Loaded {len(sft_dataset)} existing data points.")

        # [Embedding Logic]
        # if embedding_model_instance and sft_dataset:
        #     # Code to re-populate vector DB
        #     pass

    while len(sft_dataset) < TARGET_DATASET_SIZE:
        
        logger.info(f"Progress: {len(sft_dataset)} / {TARGET_DATASET_SIZE}")
        
        num_samples = min(3, len(sft_dataset))
        recent_examples_for_feedback = random.sample(sft_dataset, num_samples) if sft_dataset else []
        prompt = generate_diverse_cls_prompt(recent_examples_for_feedback)
        
        response_content = None
        
        try:
            logger.info("Sending a single request to DeepSeek API...")
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "system", "content": "You are an expert assistant for intent classification data generation."}, {"role": "user", "content": prompt}],
                stream=False,
                timeout=120.0
            )
            response_content = response.choices[0].message.content
            
            cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_content.strip(), flags=re.MULTILINE)
            parsed_json = json.loads(cleaned_response)

            if "user_query" in parsed_json and "intent_class" in parsed_json:
                query = parsed_json["user_query"].strip()
                
                is_duplicate = False
                if query.lower() in existing_queries_set:
                    is_duplicate = True
                    logger.warning(f"Duplicate query (text-based) generated, skipping: '{query}'")
                
                # [Embedding Logic]
                # if not is_duplicate and embedding_model_instance:
                #     # Semantic deduplication logic would go here
                #     pass

                if not is_duplicate:
                    sft_dataset.append(parsed_json)
                    existing_queries_set.add(query.lower())
                    
                    with open(SFT_OUTPUT_FILE, "a", encoding="utf-8") as f:
                        f.write(json.dumps(parsed_json, ensure_ascii=False) + "\n")
                    
                    logger.success(f"Generated and saved new unique data point #{len(sft_dataset)}.")
                    print("-" * 20)
                    print(json.dumps(parsed_json, indent=2, ensure_ascii=False))
                    print("-" * 20)
                    
                    # [Embedding Logic]
                    # if embedding_model_instance:
                    #     # Logic to add new vector to DB
                    #     pass
            else:
                logger.warning("Generated JSON is missing required keys. Skipping.")

            time.sleep(1)

        except (APIConnectionError, RateLimitError) as e:
            logger.error(f"Network or Rate Limit Error: {e}. Retrying in 15 seconds...")
            time.sleep(15)
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from API response. Response was: \n{response_content}")
            time.sleep(5)
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}. Retrying in 20 seconds...")
            time.sleep(20)

    logger.success("--- Pipeline Complete! ---")
    logger.info(f"Target dataset size of {TARGET_DATASET_SIZE} reached.")
    logger.info(f"Dataset saved to '{SFT_OUTPUT_FILE}'.")


if __name__ == "__main__":
    main()
