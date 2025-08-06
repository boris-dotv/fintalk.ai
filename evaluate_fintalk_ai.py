# coding=utf-8
import json
import sys
import re
import os
from typing import List, Dict, Any, Set
from loguru import logger
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# --- 1. Configuration ---
# File paths for the evaluation data.
# These should point to your actual evaluation files.
PREDICTIONS_PATH = "./predictions.jsonl"
GROUND_TRUTH_PATH = "./ground_truth.jsonl"
OUTPUT_PATH = "./evaluation_results.json"

# Model for semantic fluency calculation.
# Ensure this model is downloaded and available at the specified path.
EMBEDDING_MODEL_PATH = './models/Qwen3-Embedding-8B'

# Weights for the FinTalk-AI Composite Score (FCS)
FCS_WEIGHTS = {
    "correctness": 0.4,
    "completeness": 0.3,
    "fluency": 0.3
}

# --- 2. Helper Functions ---

def dump_to_json(data: Dict[str, Any], path: str):
    """Dumps a dictionary to a JSON file with pretty printing."""
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.success(f"Evaluation results successfully saved to: {path}")
    except Exception as e:
        logger.error(f"Failed to save results to {path}. Error: {e}")

# --- 3. The Main Evaluator Class ---

class FinTalkEvaluator:
    """
    Calculates the FinTalk-AI Composite Score (FCS) by evaluating
    Correctness, Completeness, and Fluency for a set of predictions.
    """

    def __init__(self, ground_truth_path: str, embedding_model_path: str):
        """
        Initializes the evaluator by loading the ground truth data and the embedding model.
        """
        logger.info("Initializing FinTalk.ai Evaluator...")
        self.ground_truth_data = self._load_ground_truth(ground_truth_path)
        self.embedding_model = self._load_embedding_model(embedding_model_path)

    def _load_ground_truth(self, path: str) -> Dict[str, Dict[str, Any]]:
        """Loads and indexes the ground truth data by question ID for fast lookup."""
        if not os.path.exists(path):
            logger.error(f"Ground truth file not found at: {path}")
            raise FileNotFoundError(f"Ground truth file not found: {path}")
        
        data_map = {}
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    item = json.loads(line)
                    # Use 'ID' as the unique key
                    data_map[item['ID']] = item
        logger.info(f"Loaded {len(data_map)} ground truth entries.")
        return data_map

    def _load_embedding_model(self, path: str) -> SentenceTransformer:
        """Loads the SentenceTransformer model for fluency scoring."""
        if not os.path.exists(path):
            logger.error(f"Embedding model directory not found at: {path}")
            raise FileNotFoundError(f"Model path not found. Please download '{path}'.")
        
        try:
            logger.info(f"Loading embedding model from '{path}'. This may take a moment...")
            # Using device_map="auto" is recommended for large models
            model = SentenceTransformer(path, device='cuda', trust_remote_code=True)
            logger.success("Embedding model loaded successfully.")
            return model
        except Exception as e:
            logger.error(f"Failed to load the embedding model. Error: {e}")
            raise

    def _extract_info_from_text(self, text: str) -> Set[str]:
        """A helper to extract numerical values and keywords from a text string."""
        # Find all numbers (integers and floats)
        numbers = re.findall(r'-?\d+\.\d+|-?\d+', text)
        # Find all words, convert to lowercase
        words = re.findall(r'\b\w+\b', text.lower())
        return set(numbers) | set(words)

    def _calculate_correctness(self, generated_answer: str, key_info: List[str]) -> int:
        """
        Calculates the Correctness score.
        This is a strict, all-or-nothing check. The answer must contain ALL
        key information to be considered correct.
        """
        if not key_info:
            return 1 # If there's no key info required, it's trivially correct.
        
        extracted_info = self._extract_info_from_text(generated_answer)
        
        all_found = True
        for key_item in key_info:
            # Check if each key_item (as a word or number) is in the extracted info
            if str(key_item).lower() not in extracted_info:
                all_found = False
                break
        
        return 1 if all_found else 0

    def _calculate_completeness(self, generated_answer: str, key_info: List[str]) -> float:
        """
        Calculates the Completeness score.
        This is a proportional score based on how many of the key info items are present.
        """
        if not key_info:
            return 1.0 # If there's no key info, it's trivially complete.
            
        extracted_info = self._extract_info_from_text(generated_answer)
        
        found_count = 0
        for key_item in key_info:
            if str(key_item).lower() in extracted_info:
                found_count += 1
                
        return found_count / len(key_info)

    def _calculate_fluency(self, generated_answer: str, reference_answers: List[str]) -> float:
        """
        Calculates the Fluency score using semantic similarity.
        It returns the highest similarity score against all reference answers.
        """
        if not reference_answers:
            return 0.0 # Cannot score fluency without references.
        
        try:
            generated_embedding = self.embedding_model.encode([generated_answer], normalize_embeddings=True)
            reference_embeddings = self.embedding_model.encode(reference_answers, normalize_embeddings=True)
            
            similarities = cosine_similarity(generated_embedding, reference_embeddings)
            
            # Return the maximum similarity score
            return float(np.max(similarities))
        except Exception as e:
            logger.error(f"Error during fluency calculation: {e}")
            return 0.0

    def evaluate(self, predictions_path: str) -> Dict[str, Any]:
        """
        Runs the full evaluation pipeline on a prediction file.
        """
        if not os.path.exists(predictions_path):
            logger.error(f"Prediction file not found at: {predictions_path}")
            raise FileNotFoundError(f"Prediction file not found: {predictions_path}")

        with open(predictions_path, 'r', encoding='utf-8') as f:
            predictions = [json.loads(line) for line in f if line.strip()]

        if len(predictions) != len(self.ground_truth_data):
            logger.warning(f"Mismatch in number of predictions ({len(predictions)}) "
                           f"and ground truth entries ({len(self.ground_truth_data)}).")
        
        detailed_scores = {}
        all_fcs_scores = []

        for pred in predictions:
            pred_id = pred.get("ID")
            if pred_id not in self.ground_truth_data:
                logger.warning(f"Prediction with ID '{pred_id}' not found in ground truth. Skipping.")
                continue

            ground_truth = self.ground_truth_data[pred_id]
            generated_answer = pred.get("answer", "")
            key_info = ground_truth.get("key_info", [])
            reference_answers = ground_truth.get("reference_answers", [])

            # Calculate individual scores
            score_correctness = self._calculate_correctness(generated_answer, key_info)
            score_completeness = self._calculate_completeness(generated_answer, key_info)
            score_fluency = self._calculate_fluency(generated_answer, reference_answers)

            # Calculate the final FinTalk-AI Composite Score (FCS)
            fcs = (FCS_WEIGHTS["correctness"] * score_correctness +
                   FCS_WEIGHTS["completeness"] * score_completeness +
                   FCS_WEIGHTS["fluency"] * score_fluency)
            
            all_fcs_scores.append(fcs)
            detailed_scores[pred_id] = {
                "question": ground_truth["question"],
                "generated_answer": generated_answer,
                "score_correctness": score_correctness,
                "score_completeness": score_completeness,
                "score_fluency": round(score_fluency, 4),
                "FCS": round(fcs, 4)
            }
        
        # Calculate overall average score
        average_fcs = np.mean(all_fcs_scores) if all_fcs_scores else 0.0
        
        final_results = {
            "success": True,
            "overall_average_FCS": round(average_fcs, 4),
            "scoreJson": {
                "score": round(average_fcs, 4),
                "average_correctness": round(np.mean([s['score_correctness'] for s in detailed_scores.values()]), 4),
                "average_completeness": round(np.mean([s['score_completeness'] for s in detailed_scores.values()]), 4),
                "average_fluency": round(np.mean([s['score_fluency'] for s in detailed_scores.values()]), 4)
            },
            "detailed_scores": detailed_scores
        }
        
        return final_results


def create_dummy_files_for_testing():
    """Creates dummy data files to make the script runnable out-of-the-box."""
    logger.info("Creating dummy data files for demonstration...")
    
    # Dummy Ground Truth
    gt_data = [
        {"ID": 101, "question": "What is the employee size of 'Ramp'?", "key_info": ["1174"], "reference_answers": ["Ramp has 1174 employees.", "The employee size for Ramp is 1174."]},
        {"ID": 201, "question": "What are the staff counts of 'Ramp' and 'Cora'?", "key_info": ["1174", "504"], "reference_answers": ["Ramp has 1174 employees, while Cora has 504.", "The staff counts are 1174 for Ramp and 504 for Cora."]}
    ]
    with open(GROUND_TRUTH_PATH, 'w', encoding='utf-8') as f:
        for item in gt_data:
            f.write(json.dumps(item) + '\n')

    # Dummy Predictions
    pred_data = [
        {"ID": 101, "answer": "The employee size for Ramp is 1174."}, # Perfect answer
        {"ID": 201, "answer": "The staff count for Ramp is 1174."}      # Partially correct answer
    ]
    with open(PREDICTIONS_PATH, 'w', encoding='utf-8') as f:
        for item in pred_data:
            f.write(json.dumps(item) + '\n')

if __name__ == "__main__":
    
    # This block creates dummy files if they don't exist, for easy testing.
    # In your real workflow, you would provide your own files.
    if not os.path.exists(GROUND_TRUTH_PATH) or not os.path.exists(PREDICTIONS_PATH):
        create_dummy_files_for_testing()

    try:
        # Initialize and run the evaluator
        evaluator = FinTalkEvaluator(
            ground_truth_path=GROUND_TRUTH_PATH,
            embedding_model_path=EMBEDDING_MODEL_PATH
        )
        final_report = evaluator.evaluate(predictions_path=PREDICTIONS_PATH)
        
        # Save the final report
        dump_to_json(final_report, OUTPUT_PATH)
        
        # Print a summary to the console
        print("\n" + "="*50)
        print(" EVALUATION SUMMARY ".center(50, "="))
        print("="*50)
        print(f"Overall Average FCS: {final_report['overall_average_FCS']}")
        print(f"  - Avg Correctness: {final_report['scoreJson']['average_correctness']}")
        print(f"  - Avg Completeness: {final_report['scoreJson']['average_completeness']}")
        print(f"  - Avg Fluency: {final_report['scoreJson']['average_fluency']}")
        print("="*50)

    except FileNotFoundError:
        logger.error("Evaluation stopped. A required file or model was not found.")
        logger.error(f"Please ensure '{GROUND_TRUTH_PATH}', '{PREDICTIONS_PATH}', and the model at '{EMBEDDING_MODEL_PATH}' exist.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during evaluation: {e}")
        # Create an error report file
        error_report = {
            "success": False,
            "score": 0,
            "errorMessage": "An unexpected error occurred during evaluation.",
            "errorDetail": str(e)
        }
        dump_to_json(error_report, OUTPUT_PATH)