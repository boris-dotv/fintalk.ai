import json
import random
import re
import pandas as pd

# List of years
year_list = ['2019', '2020', '2021', '2022']

# List of cities/provinces
city_list = [
    'Guangdong Province', 'Shenzhen', 'Shanghai', 'Beijing City', 'Shanghai City', 
    'Shenzhen City', 'Beijing', 'Hangzhou', 'Changsha', 'Guangzhou', 'Chengdu', 
    'Chongqing', 'Jiangsu', 'Sichuan'
]

# List of financial/company fields (metric 1)
field1_list = [
    'Subtotal of Cash Inflows from Operating Activities', 'Fixed Assets', 'Taxes and Fees Paid', 
    'Subtotal of Cash Outflows from Operating Activities', 'Subtotal of Cash Outflows from Investing Activities',
    'Taxes Payable', 'Employee Benefits Payable', 'Undistributed Profits', 'Total Liabilities', 
    'Total Assets', 'Taxes and Surcharges', 'Intangible Assets', 'Subtotal of Cash Outflows from Financing Activities',
    'Subtotal of Cash Inflows from Investing Activities', 'Cash and Cash Equivalents', 'Capital Reserve', 
    'Net Cash Flow from Operating Activities', 'Net Cash Flow from Investing Activities',
    'Cash Received Related to Other Operating Activities', 'Interest Income', 'Operating Income', 'Non-Operating Expenses',
    'Cash Paid Related to Other Operating Activities', 'Net Cash Flow from Financing Activities', 'Surplus Reserve', 
    'Operating Profit', 'Non-Operating Income', 'Income Tax Expense', 'Other Income',
    'Net Increase in Cash and Cash Equivalents', 'Net Profit', 'Other Receivables', 'Operating Costs', 
    'Total Comprehensive Income', 'Total Current Assets', 'Accounts Receivable', 'Prepayments', 'Other Payables',
    'Total Non-Current Assets', 'Basic Earnings Per Share', 'Purchase of Goods', 'Cash Paid for Services Received', 
    'Accounts Payable', 'Total Current Liabilities', 'Total Profit', 'Administrative Expenses', 'Other Current Assets',
    'Deferred Income Tax Assets', 'Sales of Goods', 'Cash Received for Services Rendered', 
    'Cash and Cash Equivalents at End of Period', 'Financial Expenses', 'Total Operating Income',
    'Cash and Cash Equivalents at Beginning of Period', 'Total Non-Current Liabilities', 'Inventories', 
    'Distribution of Dividends', 'Cash Paid for Profits Distribution or Interest Repayment', 'Diluted Earnings Per Share', 
    'Total Owners\' Equity', 'Total Operating Costs', 'Sales Expenses', 'Total Liabilities and Owners\' Equity',
    'Subtotal of Cash Inflows from Financing Activities', 'Net Profit from Continuing Operations', 
    'Total Owners\' Equity Attributable to Parent Company', 'Credit Impairment Loss', 'Acquisition of Fixed Assets',
    'Cash Paid for Intangible Assets and Other Long-Term Assets', 'Financial Personnel', 
    'Total Comprehensive Income Attributable to Parent Company Owners', 'Sales Personnel', 'Disposal of Fixed Assets',
    'Net Cash Recovered from Disposal of Intangible Assets and Other Long-Term Assets', 'Investment Income', 
    'Administrative Personnel',
    'Number of Retired Employees Whose Costs Are Borne by Parent Company and Major Subsidiaries', 
    'Technical Personnel', 'Interest Expense', 'Production Personnel', 'R&D Expenses',
    'Asset Impairment Loss', 'Construction in Progress', 'Cash Paid Related to Other Financing Activities',
    'Asset Disposal Income', 'Long-Term Prepaid Expenses', 'Cash Paid for Debt Repayment', 'Deferred Income', 
    'Other Non-Current Assets', 'Cash Received from Borrowings', 'Cash Received from Investment Income',
    'Cash Paid for Investments', 'Tax and Fee Refunds Received', 'Short-Term Borrowings', 'Minority Interests', 
    'Total Comprehensive Income Attributable to Minority Shareholders',
    'Minority Shareholder Profit/Loss', 'Net Profit Attributable to Parent Company Shareholders',
    'Total Number of Employees', 'Effect of Exchange Rate Changes on Cash and Cash Equivalents', 
    'Number of Employees in Parent Company', 'Number of Employees in Major Subsidiaries'
]

# List of financial/company fields (metric 2, includes additional company info)
field2_list = [
    'Subtotal of Cash Inflows from Operating Activities', 'Company Abbreviation', 'Fixed Assets', 
    'Taxes and Fees Paid', 'Subtotal of Cash Outflows from Operating Activities',
    'Subtotal of Cash Outflows from Investing Activities', 'Taxes Payable',
    'Employee Benefits Payable', 'Undistributed Profits', 'Total Liabilities', 'Email Address', 
    'Total Assets', 'Taxes and Surcharges', 'Intangible Assets',
    'Company\'s Legal Representative', 'Subtotal of Cash Outflows from Financing Activities', 
    'Subtotal of Cash Inflows from Investing Activities', 'Cash and Cash Equivalents', 'Capital Reserve',
    'Net Cash Flow from Operating Activities', 'Net Cash Flow from Investing Activities', 
    'Cash Received Related to Other Operating Activities', 'Interest Income',
    'Operating Income', 'Non-Operating Expenses', 'Cash Paid Related to Other Operating Activities', 
    'Net Cash Flow from Financing Activities', 'Surplus Reserve',
    'Operating Profit', 'Non-Operating Income', 'Income Tax Expense', 'Other Income', 
    'Net Increase in Cash and Cash Equivalents', 'Net Profit', 'Other Receivables',
    'Operating Costs', 'Total Comprehensive Income', 'Total Current Assets', 'Accounts Receivable', 
    'Prepayments', 'Other Payables', 'Total Non-Current Assets',
    'Basic Earnings Per Share', 'Purchase of Goods', 'Cash Paid for Services Received', 'Accounts Payable', 
    'Total Current Liabilities', 'Total Profit', 'Administrative Expenses',
    'Other Current Assets', 'Deferred Income Tax Assets', 'Sales of Goods', 
    'Cash Received for Services Rendered', 'Cash and Cash Equivalents at End of Period', 'Financial Expenses',
    'Total Operating Income', 'Cash and Cash Equivalents at Beginning of Period', 'Total Non-Current Liabilities', 
    'Inventories', 'Distribution of Dividends', 'Cash Paid for Profits Distribution or Interest Repayment',
    'Diluted Earnings Per Share', 'Total Owners\' Equity', 'Total Operating Costs', 'Sales Expenses', 
    'Total Liabilities and Owners\' Equity', 'Subtotal of Cash Inflows from Financing Activities',
    'Net Profit from Continuing Operations', 'Total Owners\' Equity Attributable to Parent Company', 
    'Credit Impairment Loss', 'Acquisition of Fixed Assets',
    'Cash Paid for Intangible Assets and Other Long-Term Assets', 'Financial Personnel', 
    'Total Comprehensive Income Attributable to Parent Company Owners', 'Sales Personnel',
    'Disposal of Fixed Assets', 'Net Cash Recovered from Disposal of Intangible Assets and Other Long-Term Assets', 
    'Investment Income', 'Administrative Personnel',
    'Number of Retired Employees Whose Costs Are Borne by Parent Company and Major Subsidiaries', 
    'Technical Personnel', 'Interest Expense', 'Production Personnel', 'R&D Expenses',
    'Asset Impairment Loss', 'Construction in Progress', 'Cash Paid Related to Other Financing Activities',
    'Asset Disposal Income', 'Long-Term Prepaid Expenses', 'Cash Paid for Debt Repayment', 'Deferred Income', 
    'Other Non-Current Assets', 'Cash Received from Borrowings', 'Cash Received from Investment Income',
    'Cash Paid for Investments', 'Tax and Fee Refunds Received', 'Short-Term Borrowings', 'Minority Interests', 
    'Total Comprehensive Income Attributable to Minority Shareholders',
    'Minority Shareholder Profit/Loss', 'Net Profit Attributable to Parent Company Shareholders',
    'Total Number of Employees', 'Effect of Exchange Rate Changes on Cash and Cash Equivalents',
    'Number of Employees in Parent Company', 'Number of Employees in Major Subsidiaries'
]

# List of top N quantities
top_num_list = [
    ['two', '2'], ['five', '5'], ['5', '5'], ['three', '3'], ['ten', '10'], 
    ['twenty', '20'], ['10', '10'], ['fifteen', '15'], ['four', '4'], 
    ['4', '4'], ['15', '15'], ['3', '3']
]

# List of ordinal quantities (and their 0-based index)
order_num_list = [
    ['second', '1'], ['fifth', '4'], ['5th', '4'], ['third', '2'], ['tenth', '9'], 
    ['twentieth', '19'], ['10th', '9'], ['fifteenth', '14'], ['fourth', '3'], 
    ['4th', '3'], ['15th', '14'], ['3rd', '2'], ['sixth', '5'], ['seventh', '6'], 
    ['eighth', '7'], ['ninth', '8']
]

# Load original prompt data
with open('prompt.jsonl', 'r', encoding='utf-8') as f:
    test_questions_data = [json.loads(line) for line in f.readlines()]

# Initialize lists for generated questions and answers
generated_questions = []
generated_answers = []

# Generate new questions and answers by replacing placeholders
for item in test_questions_data:
    for i in range(20): # Generate 20 variations for each base question
        current_year = year_list[random.randint(0, len(year_list) - 1)]
        current_city = city_list[random.randint(0, len(city_list) - 1)]
        current_field1 = field1_list[random.randint(0, len(field1_list) - 1)]
        current_field2 = field2_list[random.randint(0, len(field2_list) - 1)]
        
        # Ensure field1 and field2 are different
        while current_field1 == current_field2:
            print('Duplicate found... retrying field2 selection')
            current_field2 = field2_list[random.randint(0, len(field2_list) - 1)]
            
        current_top_num = list(top_num_list[random.randint(0, len(top_num_list) - 1)])
        current_order_num = list(order_num_list[random.randint(0, len(order_num_list) - 1)])
        
        # Define the mapping for placeholders to actual values
        replacement_map = {
            "[YEAR]": current_year,
            "[CITY]": current_city,
            "[METRIC]": current_field1,
            "[METRIC1]": current_field1,
            "[METRIC2]": current_field2,
            "[TOP_COUNT]": current_top_num[0],
            "[TOP_COUNT_LIMIT]": current_top_num[1],
            "[NUMBER]": current_order_num[0],
            "[NUMBER_MINUS_ONE]": current_order_num[1]
        }

        # Create a regex pattern to find and replace all placeholders
        substitution_pattern = re.compile("|".join(map(re.escape, replacement_map.keys())))
        
        # Apply replacements to question and answer
        generated_questions.append(substitution_pattern.sub(lambda match_obj: replacement_map[match_obj.group()], item['question']))
        generated_answers.append(substitution_pattern.sub(lambda match_obj: replacement_map[match_obj.group()], item['answer']))

# Create a Pandas DataFrame from the generated data
output_df = pd.DataFrame()
output_df['question'] = generated_questions
output_df['answer'] = generated_answers

# Save the DataFrame to a CSV file
output_df.to_csv('tuning_prompt.csv', index=False, encoding='utf-8-sig') 