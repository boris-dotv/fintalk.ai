import re
from loguru import logger

import prompt_util


def get_years_of_question(question):
    years = re.findall(r'\d{4}', question)

    if len(years) == 1:
        # Check for patterns like 'the previous year' or '1 year ago'
        if re.search(r'((?:(?:up|before|go)(?:of)?(?:1|one)|go_up)year|(?:1|one)year(?:before|ago))', question) and 'year_before_last' not in question:
            last_year = int(years[0]) - 1
            years.append(str(last_year))
        # Check for patterns like 'the year before last' or '2 years ago'
        if re.search(r'((before|year_before_last)year|[2two]years(before|ago))', question):
            last_last_year = int(years[0]) - 2
            years.append(str(last_last_year))
        # Check for patterns like 'the previous 2 years'
        if re.search(r'(?:up|before|go)(?:of)?(?:two2)years', question):
            last_year = int(years[0]) - 1
            last_last_year = int(years[0]) - 2
            years.append(str(last_year))
            years.append(str(last_last_year))

        # Check for patterns like 'next year' or '1 year later'
        if re.search(r'((?:after|next)(?:of)?(?:1|one)year|(?:1|one)year(?:after|later|afterward))', question):
            next_year = int(years[0]) + 1
            years.append(str(next_year))
        # Check for patterns like '2 years later'
        if re.search(r'[2two]years(?:after|later|afterward)', question):
            next_next_year = int(years[0]) + 2
            years.append(str(next_next_year))
        # Check for patterns like 'the next 2 years'
        if re.search(r'(after|next|following)(?:of)?(?:two2)years', question):
            next_year = int(years[0]) + 1
            next_next_year = int(years[0]) + 2 
            years.append(str(next_year))
            years.append(str(next_next_year))

    if len(years) == 2:
        # Check for year ranges like 'YYYY to YYYY'
        if re.search(r'\d{4}(?:year)?(?:to|-|until)\d{4}(?:year)?', question):
            year0 = int(years[0])
            year1 = int(years[1])
            for year in range(min(year0, year1) + 1, max(year0, year1)):
                years.append(str(year))

    return years


def get_match_company_names(question, pdf_info):
    # Delete parentheses.
    question = re.sub(r'[()（）]', '', question) 

    matched_companys = []
    for k, v in pdf_info.items():
        company = v['company']
        abbr = v['abbr']
        if company in question:
            matched_companys.append(company)
        if abbr in question:
            matched_companys.append(abbr)
    return matched_companys


def get_match_pdf_names(question, pdf_info):
    def get_matching_substrs(a, b):
        return ''.join(set(a).intersection(b))
    
    years = get_years_of_question(question)
    match_keys = []
    for k, v in pdf_info.items():
        company = v['company']
        abbr = v['abbr']
        year = v['year'].replace('year', '').replace(' ', '') 
        if company in question and year in years:
            match_keys.append(k)
        if abbr in question and year in years:
            match_keys.append(k)
    match_keys = list(set(match_keys))
    # Years have already been fully matched, so years can be removed.
    overlap_len = [len(get_matching_substrs(x, re.sub(r'\d?', '', question))) for x in match_keys]
    match_keys = sorted(zip(match_keys, overlap_len), key=lambda x: x[1], reverse=True)
    # print(match_keys)
    if len(match_keys) > 1:
        # logger.info(question)
        # Multiple results have exactly the same overlap rate
        if len(set([t[1] for t in match_keys])) == 1:
            pass
        else:
            logger.warning('Matched multiple results: {}'.format(match_keys)) 
            match_keys = match_keys[:1]
        # for k in match_keys:
        #     print(k[0])
    match_keys = [k[0] for k in match_keys]
    return match_keys


def get_company_name_and_abbr_code_of_question(pdf_keys, pdf_info):
    company_names = []
    for pdf_key in pdf_keys:
        company_names.append((pdf_info[pdf_key]['company'], pdf_info[pdf_key]['abbr'], pdf_info[pdf_key]['code']))
    return company_names


def parse_keyword_from_answer(anoy_question, answer):
    key_words = set()
    key_word_list = answer.split('\n')
    for key_word in key_word_list:
        key_word = key_word.replace(' ', '')
        # key_word = re.sub('annual_report|report|whether', '', key_word)
        if (key_word.endswith('company') and not key_word.endswith('stock company')) or re.search(
                r'(annual_report|financial_report|whether|highest|lowest|same|equal|at_the_time|financial_data|detailed_data|unit_is|year$)', key_word): 
            continue
        if key_word.startswith('keyword'): 
            key_word = re.sub(r"keyword[1-9][:|:]", "", key_word) 
            if key_word in ['amount', 'unit','data']: 
                continue
            if  key_word in anoy_question and len(key_word) > 1:
                key_words.add(key_word)
    return list(key_words)


def anoy_question_xx(question, real_company, years):
    question_new = question
    question_new = question_new.replace(real_company, 'XX_COMPANY') 
    for year in years:
        question_new = question_new.replace(year, 'XXXX')

    return question_new


def parse_question_keywords(model, question, real_company, years):
    # Standardize question phrasing
    question = re.sub(r'[()（）]', '', question).replace('is it?', 'what is it?').replace('is it?', 'what is it?').replace('how much', 'how much') 
    anoy_question = anoy_question_xx(question, real_company, years)
    anoy_question = re.sub(r'(XX_COMPANY|XXXX_YEAR|XXXX|keep_two_decimal_places|compare|compared_to|during_reporting_period|which_company|listed_company|the_[0-9]+(high|low)|(highest|lowest)(?:of|before|after)?[0-9]+company)', '', anoy_question) 
    if anoy_question.startswith('of'): 
        anoy_question = anoy_question[1:]
    answer = model(prompt_util.prompt_get_key_word.format(anoy_question))

    key_words = parse_keyword_from_answer(anoy_question, answer)
    # If extraction fails, try again after deleting
    if len(key_words) == 0:
        anoy_question = anoy_question.replace('of', '') 
        answer = model(prompt_util.prompt_get_key_word.format(anoy_question))
        key_words = parse_keyword_from_answer(anoy_question, answer)
    if len(key_words) == 0:
        logger.warning('Unable to extract keywords') 
        key_words = [anoy_question]

    return anoy_question, key_words