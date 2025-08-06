import os

PDF_TEXT_DIR = 'pdf_docs'
ERROR_PDF_DIR = 'error_pdfs'
CLASSIFY_PTUNING_PRE_SEQ_LEN = 512
KEYWORDS_PTUNING_PRE_SEQ_LEN = 256
NL2SQL_PTUNING_PRE_SEQ_LEN = 128
NL2SQL_PTUNING_MAX_LENGTH = 2200
BASE_DIR = '/root/autodl-tmp/.v_scratchs/llm/20_financial_AI_expert/'
DATA_PATH = BASE_DIR + "data/"
NUM_PROCESSES = 64
CLASSIFY_CHECKPOINT_PATH = BASE_DIR + "ptuning/CLASSIFY_PTUNING/output/Fin-Train-chatglm2-6b-pt-512-2e-2/checkpoint-400"
NL2SQL_CHECKPOINT_PATH = BASE_DIR + "ptuning/NL2SQL_PTUNING/output/Fin-Train-chatglm2-6b-pt-128-2e-2/checkpoint-600"
KEYWORDS_CHECKPOINT_PATH = BASE_DIR + "ptuning/KEYWORDS_PTUNING/output/Fin-Train-chatglm2-6b-pt-256-2e-2/checkpoint-250"
XPDF_PATH = BASE_DIR + 'xpdf/bin64'
LLM_MODEL_DIR = DATA_PATH + 'pretrained_models/chatglm2-6b'