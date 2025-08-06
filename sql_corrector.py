# Save this file as fintalk.ai/sql_corrector.py
import re
import sqlite3
from typing import List, Dict, Tuple, Any, Optional

import numpy as np
from loguru import logger

# --- Embedding Model Logic (Prepared for integration) ---
# In the final Orchestrator script, you would pass the initialized embedding model
# and the pre-computed schema embeddings to the corrector function.

# class EmbeddingModel:
#     # ... (This would be the same class from your data generation script)
#     pass

# --- 1. Schema and Keyword Management ---

class SchemaManager:
    """A helper class to manage database schema information."""
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.schema_info = self._load_schema()
        self.all_keywords = self._get_all_keywords()
        self.keyword_embeddings = None # To be populated by an embedding model

    def _load_schema(self) -> Dict[str, List[str]]:
        """Extracts table and column names from the SQLite connection."""
        schema = {}
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = [info[1] for info in cursor.fetchall()]
            schema[table_name] = columns
        return schema

    def _get_all_keywords(self) -> List[str]:
        """Returns a flat list of all table and column names."""
        keywords = list(self.schema_info.keys())
        for columns in self.schema_info.values():
            keywords.extend(columns)
        return list(set(keywords))

    def precompute_keyword_embeddings(self, embedding_model):
        """
        Computes and stores the vector embeddings for all schema keywords.
        This should be done once at system startup.
        """
        logger.info("Pre-computing embeddings for all database schema keywords...")
        self.keyword_embeddings = embedding_model.encode(self.all_keywords, is_query=False)
        logger.success("Schema keyword embeddings are ready.")


# --- 2. SQL Correction Logic ---

def _get_sql_components(sql: str) -> Tuple[List[str], List[str]]:
    """Extracts potential field names and numbers from a SQL query."""
    # This is a simplified extractor. A more robust solution might use a SQL parsing library.
    # It extracts words that are likely to be columns or tables, and numbers in conditions.
    
    # Remove content within quotes to avoid matching string literals
    sql_no_literals = re.sub(r"\'[^\']*\'", " ", sql)
    sql_no_literals = re.sub(r"\"[^\"]*\"", " ", sql_no_literals)
    
    potential_fields = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', sql_no_literals)
    # Filter out common SQL keywords
    sql_keywords = {'select', 'from', 'where', 'join', 'on', 'group', 'by', 'having', 'order', 'as', 'and', 'or', 'not', 'in', 'is', 'null', 'like', 'count', 'avg', 'sum', 'min', 'max', 'limit', 'desc', 'asc'}
    fields = [f for f in potential_fields if f.lower() not in sql_keywords]
    
    numbers = re.findall(r'(?:[<=>]\s*)\b(\d{3,})\b', sql) # Find numbers with >3 digits following a comparator
    return fields, numbers

def _get_numbers_from_question(question: str) -> List[str]:
    """Extracts numerical values from the natural language question."""
    # This regex finds integers and floats, ignoring years (like 2023).
    # It's a simplified version of your original complex number extractor.
    numbers = re.findall(r'\b(?<!20)\d{3,}\b|\b\d+\.\d+\b', question)
    return numbers

def _find_most_similar_keyword(keyword: str, schema_manager: SchemaManager, embedding_model, threshold: float = 0.9) -> Optional[str]:
    """Finds the most similar valid schema keyword using vector embeddings."""
    if not hasattr(schema_manager, 'keyword_embeddings') or schema_manager.keyword_embeddings is None:
        logger.warning("Keyword embeddings not pre-computed. Skipping field correction.")
        return None
        
    try:
        from sklearn.metrics.pairwise import cosine_similarity
        
        keyword_embedding = embedding_model.encode([keyword], is_query=True)
        similarities = cosine_similarity(keyword_embedding, schema_manager.keyword_embeddings)[0]
        
        best_match_index = np.argmax(similarities)
        best_score = similarities[best_match_index]
        
        if best_score >= threshold:
            most_similar = schema_manager.all_keywords[best_match_index]
            logger.info(f"Found a close match for '{keyword}': '{most_similar}' (Score: {best_score:.4f})")
            return most_similar
        else:
            logger.warning(f"No sufficiently similar keyword found for '{keyword}'. Best score was {best_score:.4f}.")
            return None
    except Exception as e:
        logger.error(f"Error during embedding-based similarity search: {e}")
        return None

def correct_sql(sql: str, question: str, schema_manager: SchemaManager, embedding_model) -> str:
    """
    Attempts to correct field names and numerical values in a generated SQL query.
    This function should be called by the Orchestrator before executing the SQL.
    """
    corrected_sql = sql
    
    # --- Step 1: Field Name Correction using Embeddings ---
    fields_in_sql, numbers_in_sql = _get_sql_components(corrected_sql)
    
    for field in set(fields_in_sql): # Use set to avoid correcting the same word multiple times
        if field not in schema_manager.all_keywords:
            logger.warning(f"Potential field error detected. '{field}' is not a valid schema keyword.")
            most_similar_word = _find_most_similar_keyword(field, schema_manager, embedding_model)
            if most_similar_word:
                # Use regex to replace the incorrect field name safely (as a whole word)
                corrected_sql = re.sub(r'\b' + re.escape(field) + r'\b', most_similar_word, corrected_sql)
    
    if corrected_sql != sql:
        logger.success(f"Field correction applied. New SQL: {corrected_sql}")

    # --- Step 2: Number Correction (Conservative Logic) ---
    original_sql_for_num_check = corrected_sql
    numbers_in_question = _get_numbers_from_question(question)
    
    # This logic applies only in the specific case where the SQL has one number and the question has one number, and they differ.
    if len(numbers_in_sql) == 1 and len(numbers_in_question) == 1 and numbers_in_sql[0] != numbers_in_question[0]:
        sql_num = numbers_in_sql[0]
        q_num = numbers_in_question[0]
        logger.warning(f"Potential number mismatch detected. SQL has '{sql_num}', question has '{q_num}'. Applying correction.")
        corrected_sql = corrected_sql.replace(sql_num, q_num)

    if corrected_sql != original_sql_for_num_check:
        logger.success(f"Number correction applied. New SQL: {corrected_sql}")

    return corrected_sql


# --- Example Usage (How the Orchestrator would use this module) ---
if __name__ == '__main__':
    # This block demonstrates how the Orchestrator would set up and use the corrector.
    
    # 1. Setup a dummy database connection
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE companies (company_name TEXT, employee_size INTEGER);")
    conn.commit()

    # 2. Setup the SchemaManager
    schema_manager = SchemaManager(conn)
    logger.info(f"Loaded schema keywords: {schema_manager.all_keywords}")
    
    # 3. Setup a dummy EmbeddingModel
    class DummyEmbeddingModel:
        def encode(self, texts, is_query=False):
            # In a real run, this returns actual vectors. Here, we simulate it for the demo.
            logger.info(f"DUMMY_ENCODE for: {texts}")
            if "company_name" in schema_manager.all_keywords: # Ensure embeddings are available
                 # Simulate: 'company_nm' is very similar to 'company_name'
                if texts == ['company_nm']:
                    sims = np.zeros(len(schema_manager.all_keywords))
                    idx = schema_manager.all_keywords.index('company_name')
                    sims[idx] = 0.98 # High similarity
                    return np.array([sims]) # Encapsulate in another array for sklearn
                # Simulate: 'employees' is very different from everything
                elif texts == ['employees']:
                     return np.array([np.random.rand(len(schema_manager.all_keywords))])
            return np.random.rand(1, len(schema_manager.all_keywords))
            
    dummy_model = DummyEmbeddingModel()
    # Pre-compute embeddings for all real keywords
    schema_manager.keyword_embeddings = np.random.rand(len(schema_manager.all_keywords), 10) # Dummy embeddings

    # 4. Run Correction Scenarios
    logger.info("\n--- SCENARIO 1: Correcting a field name ---")
    bad_sql_field = "SELECT company_nm FROM companies WHERE employee_size > 500"
    question_field = "List company names with over 500 employees"
    corrected_field_sql = correct_sql(bad_sql_field, question_field, schema_manager, dummy_model)
    print(f"Original SQL: {bad_sql_field}")
    print(f"Corrected SQL: {corrected_field_sql}")

    logger.info("\n--- SCENARIO 2: Correcting a number ---")
    bad_sql_number = "SELECT company_name FROM companies WHERE employee_size > 5000"
    question_number = "Find companies with more than 500 staff"
    corrected_number_sql = correct_sql(bad_sql_number, question_number, schema_manager, dummy_model)
    print(f"Original SQL: {bad_sql_number}")
    print(f"Corrected SQL: {corrected_number_sql}")
    
    conn.close()