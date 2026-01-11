# Save this file as fintalk.ai/formula.py
import re
from loguru import logger
from typing import List, Dict, Tuple, Any

# A simple cache for the formulas to avoid reading them repeatedly
_FORMULA_CACHE = None

def get_financial_formulas() -> List[Tuple[str, str]]:
    """
    Provides a comprehensive list of predefined financial and organizational ratio formulas.
    Each formula is a tuple of (ratio_name, expression_string). These are tailored
    to the project's specific database schema.
    """
    global _FORMULA_CACHE
    if _FORMULA_CACHE is not None:
        return _FORMULA_CACHE

    formulas = [
        # --- Management & Employee Ratios ---
        'management_to_employee_ratio=Total Managers / Employee Size',
        'executive_director_ratio=Count of Executive Directors / Total Count of Directors',
        'non_executive_director_ratio=Count of Non-Executive Directors / Total Count of Directors',
        'independent_director_ratio=Count of Independent Directors / Total Count of Directors',
        'avg_managers_per_department=Total Managers / Count of Distinct Departments',
        
        # --- Shareholder Composition & Concentration Ratios ---
        'institutional_ownership_percentage=Total Institutional Share Percentage / 100',
        'finance_shareholder_percentage=Total Finance Share Percentage / 100',
        'technology_shareholder_percentage=Total Technology Share Percentage / 100',
        'retail_shareholder_percentage=Total Retail Share Percentage / 100',
        'insurance_shareholder_percentage=Total Insurance Share Percentage / 100',
        'largest_shareholder_stake=Max Share Percentage / 100',
        'top_3_shareholder_concentration=Sum of Top 3 Share Percentages / 100',
        'top_5_shareholder_concentration=Sum of Top 5 Share Percentages / 100',
        'avg_share_per_institutional_investor=Total Institutional Share Percentage / Count of Institutional Investors',

        # --- Growth Metrics (Conceptual - requires data from two periods) ---
        'employee_growth_rate=(Current Year Employees - Previous Year Employees) / Previous Year Employees',
        'management_growth_rate=(Current Year Managers - Previous Year Managers) / Previous Year Managers',
        
        # --- Company-wide Aggregations ---
        'total_employee_count_all_companies=Sum of Employee Size',
        'avg_employee_size_all_companies=Average of Employee Size',
        'total_share_percentage_by_tag=Sum of Share Percentage for Tag',
        
        # --- Hypothetical Financial Ratios (if financial data were added) ---
        # These are examples of how the library could be extended.
        'asset_liability_ratio=Total Assets / Total Liabilities',
        'debt_to_equity_ratio=Total Liabilities / Total Equity'
    ]
    _FORMULA_CACHE = [tuple(t.split('=')) for t in formulas]
    return _FORMULA_CACHE

def find_formula_for_query(query: str) -> Tuple[str, str, List[str]]:
    """
    Finds the relevant formula for a given natural language query.

    Args:
        query: The user's question.

    Returns:
        A tuple containing (formula_name, expression, required_variables)
        or (None, None, None) if no formula is found.
    """
    query_normalized = query.lower().replace(' ', '_').replace('-', '_')
    formulas = get_financial_formulas()
    for name, expression in formulas:
        if name in query_normalized:
            # Extract variables from the expression (e.g., 'A/B' -> ['A', 'B'])
            variables = re.split(r'[()*/+-]', expression)
            variables = [v.strip() for v in variables if v.strip()]
            return name, expression, variables
    return None, None, None

def calculate_from_expression(expression: str, values: Dict[str, float]) -> float:
    """
    Safely calculates the result of a mathematical expression by substituting
    variable names with their numerical values.

    Args:
        expression: The mathematical formula string (e.g., "(A - B) / B").
        values: A dictionary mapping variable names to their float values (e.g., {'A': 100.0, 'B': 80.0}).

    Returns:
        The calculated result as a float.
    
    Security Note:
        This function uses `eval()`, which can be a security risk if the expression
        is not controlled. In this framework, all expressions are hardcoded and
        pre-defined in `get_financial_formulas`, making this usage safe. 
        Do NOT use this function with arbitrary, user-provided expressions.
    """
    # Create a local copy to avoid modifying the original dict
    local_values = values.copy()

    # Sort keys by length, descending, to prevent partial replacements (e.g., "A" in "AB")
    sorted_vars = sorted(local_values.keys(), key=len, reverse=True)

    # Replace variable names in the expression with their numerical values
    for var in sorted_vars:
        expression = expression.replace(var, str(local_values[var]))
    
    try:
        # Evaluate the final mathematical expression
        result = eval(expression)
        return result
    except ZeroDivisionError:
        logger.warning(f"Attempted to divide by zero in expression '{expression}'. Returning Not-a-Number.")
        return float('nan')
    except (SyntaxError, NameError, TypeError) as e:
        logger.error(f"Failed to calculate expression '{expression}'. Error: {e}")
        return float('nan') # Return Not-a-Number on other errors

if __name__ == '__main__':
    # Example usage to demonstrate the library's capabilities
    
    logger.info("--- Testing Formula Library ---")

    # 1. Test finding a formula
    logger.info("\n[Test 1: Finding a formula for a query]")
    test_query = "can you get me the executive_director_ratio for a company?"
    name, expr, variables = find_formula_for_query(test_query)
    if name:
        logger.info(f"Query: '{test_query}'")
        logger.info(f"Found Formula Name: '{name}'")
        logger.info(f"Expression: '{expr}'")
        logger.info(f"Required Variables: {variables}")
    else:
        logger.error("Formula not found for the test query.")

    # 2. Test a calculation
    logger.info("\n[Test 2: Performing a calculation]")
    # Simulate values that would be fetched by the Orchestrator via SQL queries
    mock_values = {
        "Count of Executive Directors": 5,
        "Total Count of Directors": 9
    }
    result = calculate_from_expression(expr, mock_values)
    logger.info(f"With mock values {mock_values}, the result of '{name}' is: {result:.4f}")

    # 3. Test a growth rate calculation
    logger.info("\n[Test 3: Performing a growth rate calculation]")
    growth_name, growth_expr, growth_vars = find_formula_for_query("employee_growth_rate")
    mock_growth_values = {
        "Current Year Employees": 1174,
        "Previous Year Employees": 800
    }
    growth_result = calculate_from_expression(growth_expr, mock_growth_values)
    logger.info(f"With mock values {mock_growth_values}, the result of '{growth_name}' is: {growth_result:.4f} (or {growth_result:.2%})")

    # 4. Test ZeroDivisionError handling
    logger.info("\n[Test 4: Testing ZeroDivisionError handling]")
    mock_zero_div_values = {
        "Current Year Employees": 1174,
        "Previous Year Employees": 0
    }
    zero_div_result = calculate_from_expression(growth_expr, mock_zero_div_values)
    logger.info(f"Result with zero division: {zero_div_result}")
