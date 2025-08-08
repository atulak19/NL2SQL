from flask import Flask, render_template, request
import re
import sqlite3
import db_utils

app = Flask(__name__)

TABLE_NAME = 'employees'
SCHEMA_COLUMNS = db_utils.get_table_schema()

DEPARTMENTS = ['sales', 'it', 'hr', 'marketing']
CITIES = ['pune', 'mumbai', 'bangalore']

AGGREGATE_KEYWORDS = {
    'average salary' : 'AVG(salary)',
    'avg salary' : 'AVG(salary)',
    'total salary' : 'SUM(salary)',
    'sum salary' : 'SUM(salary)',
    'count employees' : 'COUNT(id)',
    'total employees' : 'COUNT(id)',
    'max salary' : 'MAX(salary)',
    'highest salary' : 'MAX(salary)',
    'minimum salary' : 'MIN(salary)',
    'lowest salary' : 'MIN(salary)'
}

COLUMN_MAPPINGS = {
    'employees name' : 'name',
    'name' : 'name',
    'id' : 'id',
    'salary' : 'salary',
    'city' : 'city',
    'department' : 'department'
}

# Main Parsing function will be here

def translate_nl_to_sql(nl_query):
    # (The content of this function remains unchanged from our last successful version)
    # This is where your logic for translating queries resides
    lower_query = nl_query.lower()

    select_clause_parts = []
    where_conditions = []
    group_by_clause = ""
    
    # Priority 1: Check for Aggregate Functions
    found_aggregate = False
    for agg_key, agg_sql in AGGREGATE_KEYWORDS.items():
        if agg_key in lower_query:
            select_clause_parts.append(agg_sql)
            found_aggregate = True
            break

    if found_aggregate:
        if "department" in lower_query and "department" in SCHEMA_COLUMNS:
            group_by_clause = "GROUP BY department"
            if "department" not in [col.lower() for col in select_clause_parts if not col.startswith(('AVG', 'SUM', 'COUNT', 'MAX', 'MIN'))]: 
                select_clause_parts.append("department")
        elif "city" in lower_query and "city" in SCHEMA_COLUMNS:
            group_by_clause = "GROUP BY city"
            if "city" not in [col.lower() for col in select_clause_parts if not col.startswith(('AVG', 'SUM', 'COUNT', 'MAX', 'MIN'))]:
                select_clause_parts.append("city")
    
    # Priority 2: If no aggregate, check for Explicitly Requested Columns
    else: 
        explicit_columns_requested = []
        for nl_col, db_col in COLUMN_MAPPINGS.items():
            if re.search(r'(?:show me|list|get me|find|what are|display)?(?:\s+(?:names and departments|names|departments|salaries|cities|employees))?\s*\b' + re.escape(nl_col) + r'\b', lower_query):
                if db_col in SCHEMA_COLUMNS and db_col not in explicit_columns_requested:
                    explicit_columns_requested.append(db_col)
        
        if explicit_columns_requested:
            select_clause_parts = explicit_columns_requested
        else:
            # Priority 3: Default to SELECT *
            select_clause_parts = ["*"] 
            
    if not select_clause_parts:
        select_clause = "SELECT *"
    else:
        select_clause = "SELECT " + ", ".join(select_clause_parts)

    where_conditions = []
    for dep in DEPARTMENTS:
        if re.search(r'\b' + re.escape(dep) + r'\b(?: department)?', lower_query): 
            where_conditions.append(f"LOWER(department) = '{dep}'")
            break 
    for city in CITIES:
        if re.search(r'\b' + re.escape(city) + r'\b(?: city)?', lower_query): 
            where_conditions.append(f"LOWER(city) = '{city}'")
            break 
    salary_match_gt = re.search(r'(?:salary|salaries)\s+(?:greater than|over|more than|higher than|>)\s*(\d+)', lower_query)
    if salary_match_gt:
        where_conditions.append(f"salary > {salary_match_gt.group(1)}")
    salary_match_lt = re.search(r'(?:salary|salaries)\s+(?:less than|below|lower than|<)\s*(\d+)', lower_query)
    if salary_match_lt:
        where_conditions.append(f"salary < {salary_match_lt.group(1)}")
    salary_match_eq = re.search(r'salary (?:is|equals)\s*(\d+)', lower_query)
    if salary_match_eq and not (salary_match_gt or salary_match_lt): 
        where_conditions.append(f"salary = {salary_match_eq.group(1)}")

    name_patterns = [
        r"name is\s+([a-zA-Z\s]+)",
        r"(?:of|for)\s+([A-Z][a-z]+(?: [A-Z][a-z]+){0,2})",
        r"\b([A-Z][a-z]+(?: [A-Z][a-z]+){1,2})\b" 
    ]
    
    extracted_name = None
    for pattern in name_patterns:
        match = re.search(pattern, nl_query) 
        if match:
            potential_name = match.group(1).strip()
            if not any(char.isdigit() for char in potential_name) and not any(kw in potential_name.lower() for kw in DEPARTMENTS + CITIES + list(AGGREGATE_KEYWORDS.keys()) + ['salary', 'id']):
                extracted_name = potential_name
                break 
    if extracted_name:
        where_conditions.append(f"name = '{extracted_name}'")
    
    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)
        
    sql_query = f"{select_clause} FROM {TABLE_NAME}"
    if where_clause:
        sql_query += f" {where_clause}"
    if group_by_clause:
        sql_query += f" {group_by_clause}"
    
    sql_query = sql_query.strip()
    if "SELECT" not in sql_query or "FROM" not in sql_query:
        return None, "Could not generate a valid SQL query. Please rephrase your question."
    return sql_query, None

@app.route('/', methods=['GET', 'POST'])
def index():
    user_query = ""
    sql_query = ""
    columns = [] # Renamed from coloumns to columns
    message = ""
    results = None
    
    if request.method == "POST":
        user_query = request.form.get('nl_query')
        
        if user_query:
            sql_query, error_message = translate_nl_to_sql(user_query)
            if error_message:
                print(f"Error: {error_message}")
                message = error_message # Added for web display
            elif sql_query:
                try:
                    results, columns, db_error = db_utils.fetch_data(sql_query)
                    if db_error:
                        print(f"Error: {db_error}")
                        message = db_error # Added for web display
                    elif not results:
                        print("Results not found for your query")
                        message = "No results found for your query."
                    else:
                        print("Query executed successfully")
                        message = "Query executed successfully!"
                except Exception as e: # Corrected 'exception' to 'Exception'
                    print(f"An error occurred during query execution: {e}")
                    message = f"An unexpected error occurred: {e}" # Added for web display
            else:
                print("Could not generate SQL for your query, try rephrasing!")
                message = "Could not generate SQL for your query, try rephrasing!"
        else:
            print("Error: Please enter a query")
            message = "Please enter a query."

    return render_template("index.html", user_query = user_query, sql_query=sql_query, columns=columns, message=message, results = results)

if __name__ == "__main__":
    app.run(debug=True)