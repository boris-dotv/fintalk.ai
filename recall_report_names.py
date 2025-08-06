import re
from difflib import SequenceMatcher
from loguru import logger
from config import cfg


def recall_pdf_tables(keywords, years, tables, valid_tables=None, invalid_tables=None, 
        min_match_number=3, top_k=None):
    logger.info('recall words {}'.format(keywords))
    
    valid_keywords = keywords

    matched_lines = []
    for table_row in tables:
        table_name, row_year, row_name, row_value = table_row
        row_name = row_name.replace('"', '')
        if row_year not in years:
            continue
        
        if valid_tables is not None and table_name not in valid_tables:
            continue

        if invalid_tables is not None and table_name in invalid_tables:
            continue
        
        # find exact match, only return this row
        if row_name == valid_keywords:
            matched_lines = [(table_row, len(row_name))]
            break

        tot_match_size = 0
        matches = SequenceMatcher(None, valid_keywords, row_name, autojunk=False)
        for match in matches.get_matching_blocks():
            inter_text = valid_keywords[match.a:match.a+match.size]
            tot_match_size += match.size
        if tot_match_size >= min_match_number or row_name in valid_keywords:
            matched_lines.append([table_row, tot_match_size])

    matched_lines = sorted(matched_lines, key=lambda x: x[1], reverse=True)
    matched_lines = [t[0] for t in matched_lines]
    if top_k is not None and len(matched_lines) > top_k:
        matched_lines = matched_lines[:top_k]
    return matched_lines


if __name__ == '__main__':
    from file import load_pdf_info
    from file import load_embedding
    from file import load_test_questions

    
    pdf_info = load_pdf_info()
    test_questions = load_test_questions()

    for question in test_questions[:3000]:
        recall_extact_match(question['question'], pdf_info)
