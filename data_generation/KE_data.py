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
SFT_OUTPUT_FILE = "sft_keyword_extraction_dataset.jsonl"

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
# QDRANT_COLLECTION_NAME = "sft_ke_questions_collection"
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


# --- 3. Dynamic Prompt Engineering for KE Data ---

def get_db_schema_summary() -> str:
    """Returns a simplified summary of the schema for context."""
    return """
- `companies` table has fields: `website`, `employee_size`, `size_category`, `tech_summary`.
- `management` table has fields: `management_name`, `management_title`, `management_department`, `director_type`.
- `shareholders` table has fields: `shareholder_name`, `shareholder_description`, `share_percentage`, `shareholder_tag`.
- All tables are linked by `company_sort_id`.
"""

HIGH_QUALITY_KE_EXAMPLES_POOL = [
    {
        "noisy_query": "Hey FinTalk, can you quickly find the website for Ramp for me? Thanks!",
        "structured_output": {
            "company_names": ["Ramp"],
            "management_names": [],
            "shareholder_names": [],
            "db_fields": ["website"],
            "intent": "DATA_RETRIEVAL"
        }
    },
    {
        "noisy_query": "I was reading an article about fintech and it mentioned Airstar Bank, what's their tech stack like again? I'm curious about their strategy.",
        "structured_output": {
            "company_names": ["Airstar Bank"],
            "management_names": [],
            "shareholder_names": [],
            "db_fields": ["tech_summary"],
            "intent": "DATA_RETRIEVAL"
        }
    },
    {
        "noisy_query": "Could you compare the employee size of ZA Bank and WeLab Holdings?",
        "structured_output": {
            "company_names": ["ZA Bank", "WeLab Holdings"],
            "management_names": [],
            "shareholder_names": [],
            "db_fields": ["employee_size"],
            "intent": "COMPARISON"
        }
    },
    {
        "noisy_query": "Just tell me about the board members.",
        "structured_output": {
            "company_names": [],
            "management_names": [],
            "shareholder_names": [],
            "db_fields": ["management_name", "director_type"],
            "intent": "DATA_RETRIEVAL_AMBIGUOUS_COMPANY"
        }
    },
    {
        "noisy_query": "what is a non-executive director?",
        "structured_output": {
            "company_names": [],
            "management_names": [],
            "shareholder_names": [],
            "db_fields": [],
            "intent": "GENERAL_KNOWLEDGE"
        }
    },
    {
        "noisy_query": "Find all executives at companies where Xiaomi Group is a major shareholder and also tell me what a Series C funding round is.",
        "structured_output": {
            "company_names": [],
            "management_names": [],
            "shareholder_names": ["Xiaomi Group"],
            "db_fields": ["management_name", "director_type"],
            "intent": "COMPOUND_REQUEST"
        }
    }
]

DIVERSITY_INSTRUCTIONS_POOL = [
    "Generate a very colloquial query with filler words like 'um', 'like', 'you know', and a polite closing.",
    "Create a query that asks for multiple, unrelated pieces of information about two different companies in a single sentence.",
    "Formulate a vague query that is missing a key entity, such as the company name, forcing an 'AMBIGUOUS' intent.",
    "Design a question that mixes a request for data from the database with a general knowledge question, triggering a 'COMPOUND_REQUEST' intent.",
    "Generate a query that is slightly mis-spelled or uses informal abbreviations for company or people's names.",
    "Create a multi-turn conversational query, where the user seems to be refining a previous (unstated) request."
]

def generate_diverse_ke_prompt(schema_summary: str, recent_examples: List[Dict[str, Any]]) -> str:
    """
    Constructs a dynamically randomized prompt with negative feedback
    to guide the LLM towards generating diverse KE training data.
    """
    few_shot_examples = random.sample(HIGH_QUALITY_KE_EXAMPLES_POOL, 3)
    examples_str = "\n---\n".join([f"INPUT:\n{json.dumps(ex['noisy_query'])}\n\nOUTPUT:\n{json.dumps(ex['structured_output'], indent=2)}" for ex in few_shot_examples])
    
    diversity_instruction = random.choice(DIVERSITY_INSTRUCTIONS_POOL)

    negative_feedback_str = ""
    if recent_examples:
        negative_feedback_str = "To ensure diversity, AVOID generating queries that are semantically similar to these recently generated ones:\n"
        for ex in recent_examples:
            negative_feedback_str += f"- \"{ex['noisy_query']}\"\n"
        negative_feedback_str += "\n"

    return f"""
You are an expert data pre-processor. Your task is to generate pairs of realistic, 'noisy' user queries and their corresponding clean, structured JSON representations. This JSON will be used by a master agent to decide its next action.

DATABASE CONTEXT (for realism):
{schema_summary}

POSSIBLE INTENTS:
- `DATA_RETRIEVAL`: A standard request for information from the database.
- `COMPARISON`: The user wants to compare two or more entities.
- `AGGREGATION`: The user wants a calculated value like a count, average, or sum.
- `DATA_RETRIEVAL_AMBIGUOUS_COMPANY`: The user is asking for data but did not specify which company.
- `COMPOUND_REQUEST`: The query contains multiple distinct tasks (e.g., a data request and a general knowledge question).
- `GENERAL_KNOWLEDGE`: The query is not related to the internal database.

CRITICAL INSTRUCTIONS:
1.  **Generate Noisy Input**: The 'noisy_query' should be conversational, and may contain filler words, politeness, or irrelevant context.
2.  **Produce Clean Output**: The 'structured_output' JSON must be precise, containing only the extracted, relevant entities and the correct intent.
3.  **Follow the Diversity Hint**: Pay close attention to the special instruction below to guide your creativity.
4.  **AVOID REPETITION**: {negative_feedback_str if negative_feedback_str else "Do not repeat patterns from the positive examples."}
5.  **Output Format**: Your entire output must be a single, valid JSON object with a "noisy_query" key and a "structured_output" key.

---
Here are some POSITIVE examples of the transformation I expect. Do NOT copy their patterns directly.
{examples_str}
---

**SPECIAL DIVERSITY HINT FOR THIS GENERATION**: {diversity_instruction}

Now, following all instructions and the special diversity hint, generate one new, realistic, and unique pair.
"""

# --- 4. Main Generation Pipeline ---

def main():
    """Main function to orchestrate the SFT dataset generation pipeline for KE."""
    logger.info("--- Starting KE Dataset Generation Pipeline (v3 with Dynamic Prompts) ---")
    
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
                    existing_queries_set.add(data['noisy_query'].lower().strip())
                except json.JSONDecodeError:
                    continue
        logger.info(f"Loaded {len(sft_dataset)} existing data points.")

        # [Embedding Logic]
        # if embedding_model_instance and sft_dataset:
        #     # Code to re-populate vector DB
        #     pass

    db_schema_summary = get_db_schema_summary()

    while len(sft_dataset) < TARGET_DATASET_SIZE:
        
        logger.info(f"Progress: {len(sft_dataset)} / {TARGET_DATASET_SIZE}")
        
        num_samples = min(3, len(sft_dataset))
        recent_examples_for_feedback = random.sample(sft_dataset, num_samples) if sft_dataset else []
        prompt = generate_diverse_ke_prompt(db_schema_summary, recent_examples_for_feedback)
        
        response_content = None
        
        try:
            logger.info("Sending a single request to DeepSeek API...")
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "system", "content": "You are a helpful assistant for generating structured data from noisy text."}, {"role": "user", "content": prompt}],
                stream=False,
                timeout=120.0
            )
            response_content = response.choices[0].message.content
            
            cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_content.strip(), flags=re.MULTILINE)
            parsed_json = json.loads(cleaned_response)

            if "noisy_query" in parsed_json and "structured_output" in parsed_json:
                query = parsed_json["noisy_query"].strip()
                
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
