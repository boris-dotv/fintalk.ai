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

DEEPSEEK_API_KEY = "sk-c667ae5858c34180808b7752ec6b36fd"

try:
    client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")
    logger.info("OpenAI client initialized for DeepSeek API.")
except Exception as e:
    logger.error(f"Failed to initialize OpenAI client: {e}")
    exit()

TARGET_DATASET_SIZE = 5000
SFT_OUTPUT_FILE = "sft_nl2sql_dataset.jsonl"

# --- 2. Embedding Model Logic (Prepared and Commented Out) ---


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
# QDRANT_COLLECTION_NAME = "sft_questions_collection_semantic"
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
existing_questions_set = set()


# --- 3. Dynamic Prompt Engineering for High Diversity ---

def get_db_schema() -> str:
    """Returns the database schema for the prompt."""
    return """
CREATE TABLE companies (
    company_sort_id INT PRIMARY KEY, website VARCHAR(255), employee_size INT,
    size_category VARCHAR(50), tech_summary TEXT
);
CREATE TABLE management (
    management_id INT PRIMARY KEY, company_sort_id INT, management_name VARCHAR(255),
    management_title VARCHAR(255), management_department VARCHAR(100), director_type VARCHAR(100)
);
CREATE TABLE shareholders (
    shareholder_id INT PRIMARY KEY, company_sort_id INT, shareholder_name VARCHAR(255),
    shareholder_description TEXT, share_percentage FLOAT, shareholder_tag VARCHAR(50)
);
"""

HIGH_QUALITY_EXAMPLES_POOL = [
    {
      "question": "List companies that have at least three management members whose title is 'Vice President', and provide the count of such VPs for each company.",
      "sql": "SELECT C.name, COUNT(M.management_id) AS vp_count FROM companies AS C JOIN management AS M ON C.company_sort_id = M.company_sort_id WHERE M.management_title = 'Vice President' GROUP BY C.name HAVING COUNT(M.management_id) >= 3;"
    },
    {
      "question": "What is the average share percentage held by shareholders from the 'Finance' sector in companies with a 'large' employee size category?",
      "sql": "SELECT AVG(S.share_percentage) FROM shareholders AS S WHERE S.shareholder_tag = 'Finance' AND S.company_sort_id IN (SELECT C.company_sort_id FROM companies AS C WHERE C.size_category = 'large');"
    },
    {
      "question": "Find the names of all 'Head of Finance' managers who work at companies that have any shareholder tagged as 'Investment Fund'.",
      "sql": "SELECT M.management_name FROM management AS M JOIN companies AS C ON M.company_sort_id = C.company_sort_id JOIN shareholders AS S ON C.company_sort_id = S.company_sort_id WHERE M.management_title = 'Head of Finance' AND S.shareholder_tag = 'Investment Fund';"
    },
    {
      "question": "For each company, identify the shareholder with the smallest share percentage among those owning more than 1%. List the company name, the shareholder's name, and their percentage.",
      "sql": "SELECT C.name AS company_name, S.shareholder_name, S.share_percentage FROM companies AS C JOIN shareholders AS S ON C.company_sort_id = S.company_sort_id WHERE S.share_percentage > 1 AND S.share_percentage = (SELECT MIN(S2.share_percentage) FROM shareholders AS S2 WHERE S2.company_sort_id = C.company_sort_id AND S2.share_percentage > 1);"
    },
    {
        "question": "Which companies have no shareholders from the 'Retail' sector?",
        "sql": "SELECT name FROM companies WHERE company_sort_id NOT IN (SELECT company_sort_id FROM shareholders WHERE shareholder_tag = 'Retail');"
    },
    {
        "question": "Find the management titles that exist in companies with more than 1000 employees but not in companies with 1000 or fewer employees.",
        "sql": "SELECT DISTINCT T1.management_title FROM management AS T1 JOIN companies AS C1 ON T1.company_sort_id = C1.company_sort_id WHERE C1.employee_size > 1000 EXCEPT SELECT DISTINCT T2.management_title FROM management AS T2 JOIN companies AS C2 ON T2.company_sort_id = C2.company_sort_id WHERE C2.employee_size <= 1000;"
    },
    {
        "question": "Calculate the total number of managers for each department across all companies, and only show departments with more than 10 managers in total.",
        "sql": "SELECT management_department, COUNT(management_id) AS total_managers FROM management GROUP BY management_department HAVING COUNT(management_id) > 10;"
    }
]

DIVERSITY_INSTRUCTIONS_POOL = [
    "Generate a question that requires a LEFT JOIN to find companies that might be missing certain information (e.g., companies with no listed managers).",
    "Create a complex question involving a Common Table Expression (CTE) to first filter a subset of companies and then perform aggregations on them.",
    "Formulate a question that compares an aggregation between two different subsets of data (e.g., average share percentage for 'Finance' tag vs 'Technology' tag).",
    "Design a question that uses a window function like RANK() or DENSE_RANK() to find the top N shareholders or managers within each company.",
    "Generate a query that requires finding the second highest or second lowest value (e.g., the second largest shareholder of a company)."
]

def generate_diverse_hard_sft_prompt(schema: str, recent_examples: List[Dict[str, str]]) -> str:
    """
    Constructs a dynamically randomized prompt with negative feedback
    to guide the LLM towards generating more diverse and complex SQL queries.
    """
    few_shot_examples = random.sample(HIGH_QUALITY_EXAMPLES_POOL, 3)
    examples_str = "\n---\n".join([json.dumps(ex, indent=2) for ex in few_shot_examples])
    
    diversity_instruction = random.choice(DIVERSITY_INSTRUCTIONS_POOL)

    # Add negative feedback to the prompt
    negative_feedback_str = ""
    if recent_examples:
        negative_feedback_str = "To ensure diversity, AVOID generating questions that are semantically similar to the following recently generated examples:\n"
        for ex in recent_examples:
            negative_feedback_str += f"- {ex['question']}\n"
        negative_feedback_str += "\n"


    return f"""
You are a world-class expert in financial data analysis and SQL. Your task is to generate a single, highly complex, challenging and DIVERSE pair of (natural language question, SQL query) based on the provided database schema.

DATABASE SCHEMA:
```sql
{schema}
```

CRITICAL INSTRUCTIONS:
1.  **Complexity is Key**: The query must be complex, ideally using multiple JOINs, subqueries, or advanced features.
2.  **Follow the Diversity Hint**: Pay close attention to the special instruction below to guide your creativity.
3.  **AVOID REPETITION**: {negative_feedback_str if negative_feedback_str else "Do not repeat patterns from the positive examples."}
4.  **Output Format**: Your entire output must be a single, valid JSON object with a "question" and "sql" key.

---
Here are some POSITIVE examples of the high-complexity queries to inspire you. Do NOT copy their patterns directly.
{examples_str}
---

**SPECIAL DIVERSITY HINT FOR THIS GENERATION**: {diversity_instruction}

Now, following all instructions, especially avoiding the recent examples, generate one new, highly challenging, and unique question-SQL pair.
"""

# --- 4. Main Generation Pipeline ---

def main():
    """Main function to orchestrate the SFT dataset generation pipeline."""
    logger.info("--- Starting High-Complexity and Diverse SFT Dataset Generation Pipeline (v3 with Dynamic Prompts) ---")
    
    embedding_model_instance = None
    # [Embedding Logic]
    # try:
    #     embedding_model_instance = EmbeddingModel()
    #     setup_vector_db(embedding_model_instance.dim)
    # except Exception:
    #     logger.warning("Could not initialize embedding model. Falling back to text-based deduplication.")
    #     embedding_model_instance = None

    sft_dataset: List[Dict[str, str]] = []

    if os.path.exists(SFT_OUTPUT_FILE):
        logger.info(f"Loading existing dataset from {SFT_OUTPUT_FILE} to resume.")
        with open(SFT_OUTPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    sft_dataset.append(data)
                    existing_questions_set.add(data['question'].lower().strip())
                except json.JSONDecodeError:
                    continue
        logger.info(f"Loaded {len(sft_dataset)} existing data points.")

        # [Embedding Logic]
        # if embedding_model_instance and sft_dataset:
        #     logger.info("Populating vector DB with existing questions for semantic deduplication...")
        #     existing_questions = [item['question'] for item in sft_dataset]
        #     existing_embeddings = embedding_model_instance.encode(existing_questions)
        #     qdrant_client.upsert(
        #         collection_name=QDRANT_COLLECTION_NAME,
        #         points=[
        #             models.PointStruct(id=i, vector=embedding.tolist())
        #             for i, embedding in enumerate(existing_embeddings)
        #         ],
        #         wait=True
        #     )
        #     logger.success(f"Vector database populated with {len(sft_dataset)} entries.")

    db_schema = get_db_schema()

    while len(sft_dataset) < TARGET_DATASET_SIZE:
        
        logger.info(f"Progress: {len(sft_dataset)} / {TARGET_DATASET_SIZE}")
        
        # Get random examples for negative feedback
        num_samples = min(3, len(sft_dataset))
        recent_examples_for_feedback = random.sample(sft_dataset, num_samples) if sft_dataset else []
        prompt = generate_diverse_hard_sft_prompt(db_schema, recent_examples_for_feedback)
        
        response_content = None
        
        try:
            logger.info("Sending a single request to DeepSeek API...")
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "system", "content": "You are an expert assistant..."}, {"role": "user", "content": prompt}],
                stream=False,
                timeout=120.0
            )
            response_content = response.choices[0].message.content
            
            cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_content.strip(), flags=re.MULTILINE)
            parsed_json = json.loads(cleaned_response)

            if "question" in parsed_json and "sql" in parsed_json:
                question = parsed_json["question"].strip()
                sql = parsed_json["sql"].strip()
                
                is_duplicate = False
                # Default to text-based deduplication
                if question.lower() in existing_questions_set:
                    is_duplicate = True
                    logger.warning(f"Duplicate question (text-based) generated, skipping: '{question}'")
                
                # [Embedding Logic]
                # if not is_duplicate and embedding_model_instance:
                #     new_embedding = embedding_model_instance.encode([question])[0]
                #     search_results = qdrant_client.search(
                #         collection_name=QDRANT_COLLECTION_NAME,
                #         query_vector=new_embedding.tolist(),
                #         limit=1,
                #         score_threshold=SIMILARITY_THRESHOLD
                #     )
                #     if search_results:
                #         is_duplicate = True
                #         logger.warning(f"Skipping semantic duplicate (Score: {search_results[0].score:.4f}): '{question}'")

                if not is_duplicate:
                    new_data_point = {"question": question, "sql": sql}
                    sft_dataset.append(new_data_point)
                    existing_questions_set.add(question.lower())
                    
                    with open(SFT_OUTPUT_FILE, "a", encoding="utf-8") as f:
                        f.write(json.dumps(new_data_point, ensure_ascii=False) + "\n")
                    
                    logger.success(f"Generated and saved new unique data point #{len(sft_dataset)}.")
                    print("-" * 20)
                    print(json.dumps(new_data_point, indent=2, ensure_ascii=False))
                    print("-" * 20)
                    
                    # [Embedding Logic]
                    # if embedding_model_instance:
                    #     new_embedding = embedding_model_instance.encode([question])[0]
                    #     qdrant_client.upsert(
                    #         collection_name=QDRANT_COLLECTION_NAME,
                    #         points=[models.PointStruct(id=len(sft_dataset), vector=new_embedding.tolist())],
                    #         wait=False
                    #     )
            else:
                logger.warning("Generated JSON is missing 'question' or 'sql' key. Skipping.")

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
