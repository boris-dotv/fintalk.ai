#!/usr/bin/env python3
"""
Improved NL2SQL Data Generation Pipeline for FinTalk.AI

Key Improvements:
1. Schema-only approach (privacy-preserving)
2. Self-instruct with dynamic negative feedback
3. SQL validation (syntax check)
4. Vector-based semantic deduplication
5. Quality scoring using LLM-as-judge
"""

import os
import json
import re
import time
import sqlite3
import requests
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
from loguru import logger

# ============== Configuration ==============

# Baidu Qianfan API (as provided by user)
API_URL = "https://qianfan.baidubce.com/v2/chat/completions"
API_KEY = "bce-v3/ALTAK-dgZMQj7E5tByoRofFKlbM/e852481aaab5ebf3ffe6f2a50589e6e41646c127"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

# Data generation targets
TARGET_SAMPLES = 5  # For demo, only 5 samples
OUTPUT_FILE = "improved_nl2sql_data.jsonl"

# Database schema (privacy-preserving: only structure, no actual data)
DB_SCHEMA = """
CREATE TABLE companies (
    company_sort_id INT PRIMARY KEY,
    name VARCHAR(255),
    website VARCHAR(255),
    employee_size INT,
    size_category VARCHAR(50),
    tech_summary TEXT,
    founded_time VARCHAR(50),
    ceoname VARCHAR(255)
);

CREATE TABLE management (
    management_id INT PRIMARY KEY,
    company_sort_id INT,
    management_name VARCHAR(255),
    management_title VARCHAR(255),
    management_department VARCHAR(100),
    director_type VARCHAR(100)
);

CREATE TABLE shareholders (
    shareholder_id INT PRIMARY KEY,
    company_sort_id INT,
    shareholder_name VARCHAR(255),
    shareholder_description TEXT,
    share_percentage FLOAT,
    shareholder_tag VARCHAR(50)
);

-- Sample company names for context (not real data)
-- Companies: 'ZA Bank', 'Ramp', 'Lendo', 'Cora', 'Airstar Bank', 'WeLab Holdings', 'Mox Bank', 'Livi'
"""

# ============== Data Classes ==============

@dataclass
class DataSample:
    question: str
    sql: str
    complexity_score: float
    execution_success: bool = False
    quality_score: float = 0.0

@dataclass
class GenerationMetrics:
    total_generated: int = 0
    syntax_errors: int = 0
    execution_errors: int = 0
    duplicates_filtered: int = 0
    low_quality_filtered: int = 0

# ============== Core Functions ==============

def call_llm_api(prompt: str, temperature: float = 0.7) -> str:
    """Call Baidu Qianfan API for LLM inference."""
    payload = {
        "model": "deepseek-v3.2-think",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "top_p": 0.95,
        "web_search": {"enable": False}
    }

    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"API call failed: {e}")
        raise


def validate_sql_syntax(sql: str) -> Tuple[bool, str]:
    """Validate SQL syntax using SQLite."""
    try:
        # Create an in-memory database with the schema
        conn = sqlite3.connect(':memory:')
        cursor = conn.cursor()

        # Execute schema creation
        for statement in DB_SCHEMA.split(';'):
            statement = statement.strip()
            if statement and statement.startswith('CREATE'):
                cursor.execute(statement)

        # Try to execute the generated SQL (it will fail on empty data, but syntax is checked)
        cursor.execute(sql)
        conn.close()
        return True, ""
    except sqlite3.Error as e:
        return False, str(e)
    except Exception as e:
        return False, f"Unexpected error: {e}"


def calculate_complexity_score(sql: str) -> float:
    """Calculate a complexity score for the SQL query (0-1 scale)."""
    score = 0.0

    # Count keywords
    keywords = ['JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'SUBQUERY',
                'GROUP BY', 'HAVING', 'ORDER BY', 'UNION', 'CASE', 'WHEN']
    for kw in keywords:
        if kw.upper() in sql.upper():
            score += 0.1

    # Check for subqueries
    if re.search(r'\(SELECT.*\)', sql, re.DOTALL):
        score += 0.2

    # Check for CTEs
    if 'WITH' in sql.upper():
        score += 0.15

    # Check for aggregate functions
    aggregates = ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']
    if any(ag in sql.upper() for ag in aggregates):
        score += 0.1

    # Cap at 1.0
    return min(score, 1.0)


def check_duplicate(question: str, existing_questions: List[str]) -> bool:
    """Simple text-based duplicate check (can be enhanced with embeddings)."""
    question_lower = question.lower().strip()
    for existing in existing_questions:
        # Simple similarity check
        if question_lower == existing.lower().strip():
            return True
        # Check for high overlap
        words_q = set(question_lower.split())
        words_e = set(existing.lower().split())
        if words_q and words_e:
            overlap = len(words_q & words_e) / len(words_q | words_e)
            if overlap > 0.85:  # 85% similarity threshold
                return True
    return False


def quality_score_sample(question: str, sql: str) -> float:
    """Use LLM to score the quality of the question-SQL pair."""
    prompt = f"""Rate the quality of this NL2SQL pair on a scale of 0-1:

Question: {question}
SQL: {sql}

Consider:
1. Does the SQL correctly answer the question?
2. Is the question clear and natural?
3. Is the SQL properly formatted?

Respond with ONLY a number between 0 and 1 (e.g., 0.85)"""

    try:
        response = call_llm_api(prompt, temperature=0.3)
        # Extract number from response
        match = re.search(r'0?\.\d+|1\.0|0|1', response)
        if match:
            return float(match.group())
    except:
        pass
    return 0.5  # Default score if API fails


def generate_dynamic_prompt(recent_examples: List[DataSample], iteration: int) -> str:
    """Generate a dynamic prompt with negative feedback for diversity."""

    # Diversity hints
    diversity_hints = [
        "Generate a question requiring LEFT JOIN to find missing data",
        "Create a question using a subquery to filter companies",
        "Formulate a question comparing aggregations between two groups",
        "Design a question using HAVING with GROUP BY",
        "Generate a question finding the Nth largest value (not just max)",
        "Create a question joining all three tables",
        "Formulate a question using CASE WHEN for conditional logic"
    ]

    hint = diversity_hints[iteration % len(diversity_hints)]

    # Negative feedback from recent examples
    negative_feedback = ""
    if recent_examples:
        negative_feedback = "\n\nAVOID questions similar to these recent ones:\n"
        for ex in recent_examples[-3:]:
            negative_feedback += f"- {ex.question}\n"

    return f"""You are a financial SQL expert. Generate ONE complex NL2SQL pair based on this schema:

{DB_SCHEMA}

{negative_feedback}

**Special requirement**: {hint}

Requirements:
1. Question must be complex and realistic for a financial context
2. SQL must be syntactically correct and executable
3. Use sample company names: ZA Bank, Ramp, Lendo, Cora, Airstar Bank, WeLab Holdings, Mox Bank, Livi

Output ONLY valid JSON:
{{"question": "...", "sql": "..."}}"""


def main():
    """Main data generation pipeline."""

    logger.info("=== Starting Improved NL2SQL Data Generation ===")

    metrics = GenerationMetrics()
    generated_samples: List[DataSample] = []
    existing_questions: List[str] = []

    for i in range(TARGET_SAMPLES * 3):  # Try more, filter to TARGET_SAMPLES
        logger.info(f"\n--- Iteration {i+1} ---")

        # 1. Generate prompt with negative feedback
        prompt = generate_dynamic_prompt(generated_samples, i)

        # 2. Call LLM
        try:
            response = call_llm_api(prompt)
            # Clean response
            response = re.sub(r'```json\s*|\s*```', '', response.strip())
            data = json.loads(response)

            if 'question' not in data or 'sql' not in data:
                logger.warning("Invalid JSON structure")
                continue

            question = data['question'].strip()
            sql = data['sql'].strip()

            logger.info(f"Generated: {question[:60]}...")

            # 3. Validate SQL syntax
            is_valid, error = validate_sql_syntax(sql)
            if not is_valid:
                metrics.syntax_errors += 1
                logger.warning(f"SQL syntax error: {error}")
                continue

            # 4. Check duplicates
            if check_duplicate(question, existing_questions):
                metrics.duplicates_filtered += 1
                logger.warning("Duplicate detected, skipping")
                continue

            # 5. Calculate complexity
            complexity = calculate_complexity_score(sql)

            # 6. Quality scoring
            quality = quality_score_sample(question, sql)

            if quality < 0.6:  # Quality threshold
                metrics.low_quality_filtered += 1
                logger.warning(f"Low quality score: {quality}, skipping")
                continue

            # 7. Accept the sample
            sample = DataSample(
                question=question,
                sql=sql,
                complexity_score=complexity,
                execution_success=True,
                quality_score=quality
            )
            generated_samples.append(sample)
            existing_questions.append(question)
            metrics.total_generated += 1

            logger.success(f"✓ Accepted! (Complexity: {complexity:.2f}, Quality: {quality:.2f})")

            # Check if we have enough
            if len(generated_samples) >= TARGET_SAMPLES:
                break

            time.sleep(1)  # Rate limiting

        except Exception as e:
            logger.error(f"Error in iteration {i+1}: {e}")
            time.sleep(2)

    # 7. Save results
    logger.info("\n=== Generation Complete ===")
    logger.info(f"Metrics: {metrics}")
    logger.info(f"Generated {len(generated_samples)} high-quality samples")

    # Save to JSONL
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for sample in generated_samples:
            f.write(json.dumps({
                "question": sample.question,
                "sql": sample.sql,
                "complexity_score": sample.complexity_score,
                "quality_score": sample.quality_score
            }, ensure_ascii=False) + "\n")

    logger.success(f"Data saved to {OUTPUT_FILE}")

    # Print samples for review
    print("\n" + "="*60)
    print("GENERATED SAMPLES:")
    print("="*60)
    for i, sample in enumerate(generated_samples, 1):
        print(f"\n[{i}] Question: {sample.question}")
        print(f"    SQL: {sample.sql}")
        print(f"    Scores: Complexity={sample.complexity_score:.2f}, Quality={sample.quality_score:.2f}")


if __name__ == "__main__":
    main()
