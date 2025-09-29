

# # # from flask import Blueprint, request, jsonify
# # # import time

# # # # Import your existing modules
# # # try:
# # #     from services.query_engine import handle_query
# # #     from routes.schema import current_schema
# # #     from routes.ingestion import documents_index
# # #     IMPORTS_OK = True
# # # except ImportError as e:
# # #     print(f"Import error: {e}")
# # #     IMPORTS_OK = False

# # # # Create the blueprint
# # # query_bp = Blueprint("query", __name__)
# # # query_history = []

# # # @query_bp.route("/query", methods=["POST"])
# # # def query():
# # #     if not IMPORTS_OK:
# # #         return jsonify({
# # #             "ok": False, 
# # #             "error": "system_error",
# # #             "message": "Required modules not available"
# # #         }), 500
    
# # #     try:
# # #         data = request.json
# # #         if not data:
# # #             return jsonify({
# # #                 "ok": False,
# # #                 "error": "no_data", 
# # #                 "message": "No JSON data provided"
# # #             }), 400
        
# # #         user_query = data.get("query", "").strip()
# # #         if not user_query:
# # #             return jsonify({
# # #                 "ok": False,
# # #                 "error": "empty_query",
# # #                 "message": "Query cannot be empty"
# # #             }), 400
        
# # #         start_time = time.time()
        
# # #         # Call your existing query engine
# # #         raw_result = handle_query(user_query, current_schema, documents_index)

# # #         # Process results (keep your original logic)
# # #         doc_hits = []
# # #         if raw_result and "documents" in raw_result:
# # #             for doc in raw_result["documents"]:
# # #                 doc_hits.append({
# # #                     "doc_name": doc.get("filename", "unknown"),
# # #                     "chunk_text": doc.get("chunk", ""),
# # #                     "score": doc.get("score", 0)
# # #                 })

# # #         # Sort by score (higher = better)
# # #         doc_hits.sort(key=lambda x: x["score"], reverse=True)

# # #         # Create response
# # #         response = {
# # #             "ok": True,
# # #             "result": {
# # #                 "type": "document",
# # #                 "documents": {"hits": doc_hits},
# # #                 "db": raw_result.get("db", {}) if raw_result else {}
# # #             },
# # #             "time_ms": int((time.time() - start_time) * 1000),
# # #             "cached": False
# # #         }

# # #         # Store in history
# # #         query_history.append({"query": user_query, "result": response})
        
# # #         return jsonify(response)
        
# # #     except Exception as e:
# # #         return jsonify({
# # #             "ok": False, 
# # #             "error": "execution_error",
# # #             "message": str(e)
# # #         }), 500

# # # # Simple test endpoint
# # # @query_bp.route("/test", methods=["GET"])
# # # def test():
# # #     return jsonify({
# # #         "ok": True,
# # #         "message": "Query blueprint is working",
# # #         "imports_ok": IMPORTS_OK,
# # #         "timestamp": int(time.time())
# # #     })


# # from flask import Blueprint, request, jsonify
# # import time
# # import logging
# # from sqlalchemy import text
# # from datetime import datetime
# # import re

# # logger = logging.getLogger(__name__)

# # # Import your existing modules
# # try:
# #     from services.query_engine import handle_query as handle_document_query
# #     from routes.schema import current_schema, get_db_state
# #     from routes.ingestion import documents_index
# #     IMPORTS_OK = True
# # except ImportError as e:
# #     print(f"Import error: {e}")
# #     IMPORTS_OK = False

# # query_bp = Blueprint("query", __name__)
# # query_history = []



# # @query_bp.route("/query", methods=["POST"])
# # def query():
# #     """Process natural language queries - supports both documents and database"""
# #     if not IMPORTS_OK:
# #         return jsonify({
# #             "ok": False, 
# #             "error": "system_error",
# #             "message": "Required modules not available"
# #         }), 500
    
# #     try:
# #         data = request.json
# #         if not data:
# #             return jsonify({
# #                 "ok": False,
# #                 "error": "no_data", 
# #                 "message": "No JSON data provided"
# #             }), 400
        
# #         user_query = data.get("query", "").strip()
# #         if not user_query:
# #             return jsonify({
# #                 "ok": False,
# #                 "error": "empty_query",
# #                 "message": "Query cannot be empty"
# #             }), 400
        
# #         start_time = time.time()
# #         query_lower = user_query.lower()
        
# #         # Detect query type based on keywords
# #         is_document_query = any(word in query_lower for word in 
# #             ['resume', 'cv', 'document', 'find', 'mentioning', 'skills'])
# #         is_database_query = any(word in query_lower for word in 
# #             ['employee', 'salary', 'department', 'count', 'average', 'top', 'highest', 
# #              'paid', 'hired', 'list', 'show', 'reports', 'who', 'manager'])
        
# #         db_state = get_db_state()
# #         has_database = db_state['connected'] and db_state['schema']
# #         has_documents = documents_index and len(documents_index) > 0
        
# #         logger.info(f"Query: {user_query}")
# #         logger.info(f"Document query: {is_document_query}, DB query: {is_database_query}")
# #         logger.info(f"Has database: {has_database}, Has documents: {has_documents}")
        
# #         # Route 1: Document-only query
# #         if is_document_query and has_documents and not is_database_query:
# #             logger.info("Processing as DOCUMENT query")
# #             raw_result = handle_document_query(user_query, current_schema, documents_index)
            
# #             doc_hits = []
# #             if raw_result and "documents" in raw_result:
# #                 for doc in raw_result["documents"]:
# #                     doc_hits.append({
# #                         "doc_name": doc.get("filename", "unknown"),
# #                         "chunk_text": doc.get("chunk", ""),
# #                         "score": doc.get("score", 0)
# #                     })
            
# #             doc_hits.sort(key=lambda x: x["score"], reverse=True)
            
# #             response = {
# #                 "ok": True,
# #                 "result": {
# #                     "type": "document",
# #                     "documents": {"hits": doc_hits, "total": len(doc_hits)},
# #                     "db": {}
# #                 },
# #                 "time_ms": int((time.time() - start_time) * 1000),
# #                 "cached": False
# #             }
            
# #             query_history.append({"query": user_query, "result": response})
# #             return jsonify(response)
        
# #         # Route 2: Database-only query
# #         elif is_database_query and has_database:
# #             logger.info("Processing as DATABASE query")
            
# #             sql_result = generate_sql_for_query(user_query, db_state['schema'])
            
# #             if sql_result['sql']:
# #                 db_results = execute_sql_query(sql_result['sql'], db_state['engine'])
                
# #                 response = {
# #                     "ok": True,
# #                     "result": {
# #                         "type": "database",
# #                         "query_type": sql_result['query_type'],
# #                         "sql_generated": sql_result['sql'],
# #                         "database_results": db_results,
# #                         "documents": {"hits": [], "total": 0},
# #                         "debug_info": sql_result.get('debug_info', {})
# #                     },
# #                     "time_ms": int((time.time() - start_time) * 1000),
# #                     "cached": False
# #                 }
                
# #                 query_history.append({"query": user_query, "result": response})
# #                 return jsonify(response)
# #             else:
# #                 return jsonify({
# #                     "ok": False,
# #                     "error": "query_not_understood",
# #                     "message": sql_result.get('error_message', "Could not generate SQL for this query"),
# #                     "debug_info": sql_result.get('debug_info', {})
# #                 }), 400
        
# #         # Route 3: Hybrid or fallback
# #         else:
# #             if has_documents:
# #                 logger.info("Falling back to DOCUMENT search")
# #                 raw_result = handle_document_query(user_query, current_schema, documents_index)
                
# #                 doc_hits = []
# #                 if raw_result and "documents" in raw_result:
# #                     for doc in raw_result["documents"]:
# #                         doc_hits.append({
# #                             "doc_name": doc.get("filename", "unknown"),
# #                             "chunk_text": doc.get("chunk", ""),
# #                             "score": doc.get("score", 0)
# #                         })
                
# #                 doc_hits.sort(key=lambda x: x["score"], reverse=True)
                
# #                 response = {
# #                     "ok": True,
# #                     "result": {
# #                         "type": "document",
# #                         "documents": {"hits": doc_hits, "total": len(doc_hits)},
# #                         "db": {}
# #                     },
# #                     "time_ms": int((time.time() - start_time) * 1000),
# #                     "cached": False
# #                 }
                
# #                 query_history.append({"query": user_query, "result": response})
# #                 return jsonify(response)
# #             else:
# #                 return jsonify({
# #                     "ok": False,
# #                     "error": "no_data_source",
# #                     "message": "Please connect to database or upload documents first"
# #                 }), 400
        
# #     except Exception as e:
# #         logger.error(f"Query error: {e}", exc_info=True)
# #         return jsonify({
# #             "ok": False, 
# #             "error": "execution_error",
# #             "message": str(e)
# #         }), 500

# # def generate_sql_for_query(user_query, schema):
# #     """
# #     Robust SQL generator for multiple schema variations.
# #     Returns dict with keys: sql, query_type, table_used, debug_info, error_message
# #     """
# #     query_lower = (user_query or "").strip().lower()
# #     debug_info = {"query": user_query, "available_tables": list(schema.keys())}
# #     sql = None
# #     query_type = "select"
# #     error_message = None
# #     current_year = datetime.now().year

# #     # helper: find table by keywords
# #     def find_table_by_keywords(keys):
# #         for t in schema.keys():
# #             for k in keys:
# #                 if k in t.lower():
# #                     return t
# #         return None

# #     # Choose employee & department-like tables
# #     employee_table = find_table_by_keywords(['employee', 'staff', 'personnel', 'person']) or list(schema.keys())[0]
# #     department_table = find_table_by_keywords(['department', 'dept', 'division', 'divisions'])  # may be None

# #     emp_cols = schema.get(employee_table, [])
# #     dept_cols = schema.get(department_table, []) if department_table else []

# #     # find useful columns in employee table
# #     emp_id_col      = find_column_by_pattern(emp_cols, ['emp_id', 'id', 'person_id', 'staff_id', 'employee_id'])
# #     name_col        = find_column_by_pattern(emp_cols, ['full_name', 'employee_name', 'name'])
# #     dept_fk_col     = find_column_by_pattern(emp_cols, ['dept_id', 'department_id', 'division', 'division_code', 'dept'])
# #     salary_col      = find_column_by_pattern(emp_cols, ['salary', 'compensation', 'pay_rate', 'annual_salary', 'pay'])
# #     join_date_col   = find_column_by_pattern(emp_cols, ['join_date', 'hired_on', 'start_date', 'hire_date', 'joined_on'])
# #     position_col    = find_column_by_pattern(emp_cols, ['position', 'title', 'role', 'job_title'])
# #     reports_col     = find_column_by_pattern(emp_cols, ['reports_to', 'manager_id', 'supervisor_id', 'reports'])

# #     # find useful columns in department table (if exists)
# #     dept_id_col     = find_column_by_pattern(dept_cols, ['dept_id', 'department_id', 'id', 'division_code', 'code'])
# #     dept_name_col   = find_column_by_pattern(dept_cols, ['dept_name', 'department_name', 'division_name', 'name'])
# #     dept_manager_col= find_column_by_pattern(dept_cols, ['manager_id', 'head_id', 'manager', 'lead_id'])

# #     # also detect skills-like column (for skill+salary queries)
# #     skills_col      = find_column_by_pattern(emp_cols, ['skill', 'skills', 'expertise', 'skillset'])

# #     debug_info['detected'] = {
# #         "employee_table": employee_table,
# #         "department_table": department_table,
# #         "emp_id_col": emp_id_col,
# #         "name_col": name_col,
# #         "dept_fk_col": dept_fk_col,
# #         "salary_col": salary_col,
# #         "join_date_col": join_date_col,
# #         "position_col": position_col,
# #         "reports_col": reports_col,
# #         "dept_id_col": dept_id_col,
# #         "dept_name_col": dept_name_col,
# #         "dept_manager_col": dept_manager_col,
# #         "skills_col": skills_col
# #     }

# #     # ---------- Utility to safely embed LIKE strings ----------
# #     def sql_like_escape(s: str):
# #         # remove punctuation, multiple spaces, and escape single quotes
# #         cleaned = re.sub(r'[^\w\s]', ' ', s).strip()
# #         cleaned = re.sub(r'\s+', ' ', cleaned)
# #         return cleaned.replace("'", "''")

# #     # ---------------- CASES ----------------
# #     # 1) How many employees?
# #     if re.search(r'\b(how many employees|how many people|count|number of employees)\b', query_lower):
# #         query_type = "count"
# #         sql = f"SELECT COUNT(*) AS total_employees FROM {employee_table}"

# #     # 2) Average salary by department
# #     elif 'average salary' in query_lower or re.search(r'\b(avg|average)\b.*salary', query_lower):
# #         query_type = "average_by_dept"
# #         if dept_fk_col and salary_col and department_table and dept_id_col and dept_name_col:
# #             sql = (
# #                 f"SELECT d.{dept_name_col} AS department, AVG(e.{salary_col}) AS avg_salary "
# #                 f"FROM {employee_table} e "
# #                 f"JOIN {department_table} d ON e.{dept_fk_col} = d.{dept_id_col} "
# #                 f"WHERE e.{salary_col} IS NOT NULL "
# #                 f"GROUP BY d.{dept_name_col} ORDER BY avg_salary DESC"
# #             )
# #         elif salary_col and dept_fk_col:
# #             sql = f"SELECT {dept_fk_col} AS department_key, AVG({salary_col}) AS avg_salary FROM {employee_table} GROUP BY {dept_fk_col}"
# #         else:
# #             error_message = "No department or salary columns detected to compute averages"

# #     # 3) List employees hired this year
# #     elif any(w in query_lower for w in ['hired this year', 'joined this year', 'hired in'] ) and join_date_col:
# #         query_type = "hired_this_year"
# #         sql = f"SELECT {name_col or '*'}, {position_col or 'NULL'}, {join_date_col} FROM {employee_table} WHERE YEAR({join_date_col}) = {current_year}"

# #     # 4) Who reports to X?
# #     elif re.search(r'\breports to\b|\breporting to\b|\bwho reports to\b|\bmanaged by\b', query_lower):
# #         query_type = "reports_to"
# #         # extract manager name (everything after the phrase)
# #         m = re.search(r'(?:who reports to|reports to|reporting to|managed by)\s+(.+)$', query_lower)
# #         manager_name_raw = m.group(1).strip() if m else None
# #         manager_name_clean = sql_like_escape(manager_name_raw or "")
# #         debug_info['manager_name_raw'] = manager_name_raw
# #         debug_info['manager_name_clean'] = manager_name_clean

# #         if not manager_name_clean:
# #             error_message = "Could not parse manager name from query"
# #         elif not emp_id_col or not name_col:
# #             error_message = "Required employee id/name columns missing"
# #         else:
# #             # Option A: direct reports column on employee table (e.g., reports_to)
# #             if reports_col:
# #                 sql = (
# #                     f"SELECT e.{name_col} { (', e.'+position_col) if position_col else '' } "
# #                     f"FROM {employee_table} e "
# #                     f"WHERE e.{reports_col} = ("
# #                     f"SELECT m.{emp_id_col} FROM {employee_table} m "
# #                     f"WHERE LOWER(m.{name_col}) LIKE LOWER('%{manager_name_clean}%') LIMIT 1"
# #                     f")"
# #                 )
# #             # Option B: department-based manager stored in department table
# #             elif department_table and dept_manager_col and dept_fk_col and dept_id_col:
# #                 # join on the correct department id column (dept_id_col)
# #                 sql = (
# #                     f"SELECT e.{name_col}"
# #                     + (f", e.{position_col}" if position_col else "")
# #                     + (f", d.{dept_name_col}" if dept_name_col else "")
# #                     + f" FROM {employee_table} e "
# #                     f"JOIN {department_table} d ON e.{dept_fk_col} = d.{dept_id_col} "
# #                     f"WHERE d.{dept_manager_col} = ("
# #                     f"SELECT m.{emp_id_col} FROM {employee_table} m "
# #                     f"WHERE LOWER(m.{name_col}) LIKE LOWER('%{manager_name_clean}%') LIMIT 1"
# #                     f") AND e.{emp_id_col} != ("
# #                     f"SELECT m.{emp_id_col} FROM {employee_table} m WHERE LOWER(m.{name_col}) LIKE LOWER('%{manager_name_clean}%') LIMIT 1"
# #                     f")"
# #                 )
# #             else:
# #                 error_message = "Database schema doesn't provide reports_to or department.manager mapping"

# #     # 5) Top N highest paid employees in each department
# #     elif re.search(r'\btop\b.*\bhighest\b.*\bpaid\b', query_lower) and salary_col:
# #         query_type = "top_n_by_dept"
# #         # attempt to produce per-department top-5 using ROW_NUMBER()
# #         if dept_fk_col and salary_col:
# #             # include dept name if available
# #             inner_select = (
# #                 f"SELECT e.{dept_fk_col} AS dept_key, e.{name_col} AS emp_name, e.{salary_col} AS emp_salary, "
# #                 f"ROW_NUMBER() OVER (PARTITION BY e.{dept_fk_col} ORDER BY e.{salary_col} DESC) AS rn "
# #                 f"FROM {employee_table} e"
# #             )
# #             sql = f"SELECT * FROM ({inner_select}) t WHERE t.rn <= 5"
# #             if department_table and dept_id_col and dept_name_col:
# #                 sql = f"SELECT d.{dept_name_col} AS department, t.emp_name, t.emp_salary FROM ({inner_select}) t JOIN {department_table} d ON t.dept_key = d.{dept_id_col} WHERE t.rn <= 5 ORDER BY d.{dept_name_col}, t.emp_salary DESC"
# #         else:
# #             # fallback: top 5 overall
# #             sql = f"SELECT {name_col}, {salary_col} FROM {employee_table} WHERE {salary_col} IS NOT NULL ORDER BY {salary_col} DESC LIMIT 5"

# #     # 6) Employees with skill X earning over Y
# #     elif re.search(r'\bpython\b|\bjava\b|\br\b', query_lower) and salary_col:
# #         query_type = "skill_salary_filter"
# #         # parse salary threshold (e.g., 100k, 100000)
# #         salary_match = re.search(r'(\d{2,6})(k)?', query_lower)
# #         threshold = None
# #         if salary_match:
# #             num = int(salary_match.group(1))
# #             if salary_match.group(2) == 'k':
# #                 threshold = num * 1000
# #             else:
# #                 threshold = num
# #         # find skill token(s)
# #         skill = None
# #         for token in ['python', 'java', 'r', 'sql', 'kubernetes', 'ml', 'machine learning']:
# #             if token in query_lower:
# #                 skill = token
# #                 break
# #         # prefer using skills column if exists
# #         if skills_col:
# #             if threshold:
# #                 sql = f"SELECT {name_col}, {salary_col}, {skills_col} FROM {employee_table} WHERE LOWER({skills_col}) LIKE LOWER('%{skill}%') AND {salary_col} > {threshold}"
# #             else:
# #                 sql = f"SELECT {name_col}, {salary_col}, {skills_col} FROM {employee_table} WHERE LOWER({skills_col}) LIKE LOWER('%{skill}%')"
# #         # else, if a documents table exists that links staff -> docs (e.g., documents with staff_id), let caller handle hybrid (we can't join reliably)
# #         else:
# #             error_message = "No skills column detected in employee table. Consider using document search for CVs or add documents table mapping."

# #     # 7) Performance reviews for engineers hired last year (requires documents table)
# #     elif 'performance review' in query_lower and department_table:
# #         # look for documents table (common name 'documents')
# #         doc_table = find_table_by_keywords(['document', 'doc', 'reviews', 'performance'])
# #         if doc_table and emp_id_col:
# #             query_type = "performance_reviews"
# #             # attempt join if documents table has staff_id or person reference
# #             doc_cols = schema.get(doc_table, [])
# #             doc_staff_fk = find_column_by_pattern(doc_cols, ['staff_id', 'person_id', 'emp_id', 'employee_id', 'staffid'])
# #             if doc_staff_fk:
# #                 sql = (
# #                     f"SELECT e.{name_col}, d.{doc_staff_fk}, d.content "
# #                     f"FROM {employee_table} e JOIN {doc_table} d ON e.{emp_id_col} = d.{doc_staff_fk} "
# #                     f"WHERE YEAR(e.{join_date_col}) = {current_year - 1} AND LOWER(d.type) LIKE '%review%'"
# #                 )
# #             else:
# #                 error_message = "Documents exist but no staff/document linkage column found"

# #     # 8) Turnover / Hiring trends
# #     elif 'turnover' in query_lower:
# #         query_type = "turnover"
# #         # Turnover requires exit/termination column — detect
# #         exit_col = find_column_by_pattern(emp_cols, ['exit_date', 'termination_date', 'resignation_date', 'left_on'])
# #         if exit_col and dept_fk_col:
# #             sql = f"SELECT {dept_fk_col} AS dept_key, COUNT(*) AS exits FROM {employee_table} WHERE {exit_col} >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) GROUP BY {dept_fk_col} ORDER BY exits DESC"
# #         else:
# #             error_message = "Turnover calculation needs exit/termination date column in employee table"

# #     elif 'hiring trend' in query_lower or 'month-over-month' in query_lower or 'month over month' in query_lower:
# #         if join_date_col:
# #             query_type = "hiring_trend"
# #             sql = f"SELECT YEAR({join_date_col}) AS year, MONTH({join_date_col}) AS month, COUNT(*) AS hires FROM {employee_table} GROUP BY YEAR({join_date_col}), MONTH({join_date_col}) ORDER BY year, month"
# #         else:
# #             error_message = "No hire/join date column found"

# #     # 9) Generic list / show queries
# #     elif any(w in query_lower for w in ['list', 'show', 'display', 'get', 'who']):
# #         query_type = "select"
# #         select_cols = [c for c in [name_col, position_col, dept_fk_col, salary_col] if c]
# #         if not select_cols:
# #             select_cols = ['*']
# #         sql = f"SELECT {', '.join(select_cols)} FROM {employee_table} LIMIT 50"

# #     # Fallback: return some rows so UI can at least show schema
# #     if not sql:
# #         if not error_message:
# #             sql = f"SELECT * FROM {employee_table} LIMIT 10"
# #         else:
# #             # if an error_message was set, still provide fallback sample
# #             sql = f"SELECT * FROM {employee_table} LIMIT 10"

# #     debug_info['final_sql_sample'] = sql
# #     return {
# #         'sql': sql,
# #         'query_type': query_type,
# #         'table_used': employee_table,
# #         'debug_info': debug_info,
# #         'error_message': error_message
# #     }


# # def find_column_by_pattern(columns, patterns):
# #     """Find column that matches any of the patterns"""
# #     columns_lower = [col.lower() for col in columns]
    
# #     for pattern in patterns:
# #         for i, col_lower in enumerate(columns_lower):
# #             if pattern in col_lower:
# #                 return columns[i]
    
# #     return None

# # def execute_sql_query(sql, engine):
# #     """Execute SQL query against connected database"""
# #     try:
# #         logger.info(f"Executing SQL: {sql}")
        
# #         with engine.connect() as conn:
# #             result = conn.execute(text(sql))
# #             rows = result.fetchall()
            
# #             data = []
# #             if rows:
# #                 columns = list(result.keys())
                
# #                 for row in rows:
# #                     row_dict = {}
# #                     for key in columns:
# #                         value = row._mapping[key]
# #                         if hasattr(value, 'isoformat'):
# #                             row_dict[key] = value.isoformat()
# #                         else:
# #                             row_dict[key] = value
# #                     data.append(row_dict)
            
# #             return {
# #                 'success': True,
# #                 'data': data,
# #                 'row_count': len(data)
# #             }
    
# #     except Exception as e:
# #         logger.error(f"SQL execution error: {e}", exc_info=True)
# #         return {
# #             'success': False,
# #             'error': str(e),
# #             'data': []
# #         }

# # @query_bp.route("/query-debug", methods=["GET"])
# # def query_debug():
# #     """Debug endpoint"""
# #     db_state = get_db_state()
# #     return jsonify({
# #         "ok": True,
# #         "message": "Query blueprint is working!",
# #         "database_connected": db_state['connected'],
# #         "schema_tables": list(db_state['schema'].keys()) if db_state['schema'] else [],
# #         "documents_available": bool(documents_index),
# #         "document_count": len(documents_index) if documents_index else 0,
# #         "query_history_count": len(query_history)
# #     })

# # @query_bp.route("/test", methods=["GET"])
# # def test():
# #     return jsonify({
# #         "ok": True,
# #         "message": "Query blueprint is working",
# #         "imports_ok": IMPORTS_OK,
# #         "timestamp": int(time.time())
# #     })


# from flask import Blueprint, request, jsonify
# import time
# import logging
# from sqlalchemy import text
# from datetime import datetime
# import re
# import hashlib
# from collections import OrderedDict

# logger = logging.getLogger(__name__)

# try:
#     from services.query_engine import handle_query as handle_document_query
#     from routes.schema import current_schema, get_db_state
#     from routes.ingestion import documents_index
#     IMPORTS_OK = True
# except ImportError as e:
#     print(f"Import error: {e}")
#     IMPORTS_OK = False

# query_bp = Blueprint("query", __name__)

# # Query cache with TTL and metrics
# class QueryCache:
#     def __init__(self, max_size=1000, ttl_seconds=300):
#         self.cache = OrderedDict()
#         self.max_size = max_size
#         self.ttl_seconds = ttl_seconds
#         self.hits = 0
#         self.misses = 0
#         self.total_queries = 0
    
#     def _make_key(self, query):
#         return hashlib.md5(query.lower().encode()).hexdigest()
    
#     def get(self, query):
#         self.total_queries += 1
#         key = self._make_key(query)
        
#         if key in self.cache:
#             cached_data, timestamp = self.cache[key]
#             if time.time() - timestamp < self.ttl_seconds:
#                 self.hits += 1
#                 # Move to end (LRU)
#                 self.cache.move_to_end(key)
#                 return cached_data
#             else:
#                 del self.cache[key]
        
#         self.misses += 1
#         return None
    
#     def set(self, query, data):
#         key = self._make_key(query)
        
#         if len(self.cache) >= self.max_size:
#             self.cache.popitem(last=False)
        
#         self.cache[key] = (data, time.time())
    
#     def get_stats(self):
#         hit_rate = (self.hits / self.total_queries * 100) if self.total_queries > 0 else 0
#         return {
#             "total_queries": self.total_queries,
#             "cache_hits": self.hits,
#             "cache_misses": self.misses,
#             "hit_rate_percent": round(hit_rate, 2),
#             "cache_size": len(self.cache),
#             "max_size": self.max_size
#         }

# # Global cache instance
# query_cache = QueryCache(max_size=1000, ttl_seconds=300)
# query_history = []

# @query_bp.route("/query", methods=["POST"])
# def query():
#     """Process natural language queries with caching and metrics"""
#     if not IMPORTS_OK:
#         return jsonify({
#             "ok": False, 
#             "error": "system_error",
#             "message": "Required modules not available"
#         }), 500
    
#     try:
#         data = request.json
#         if not data:
#             return jsonify({
#                 "ok": False,
#                 "error": "no_data", 
#                 "message": "No JSON data provided"
#             }), 400
        
#         user_query = data.get("query", "").strip()
#         if not user_query:
#             return jsonify({
#                 "ok": False,
#                 "error": "empty_query",
#                 "message": "Query cannot be empty"
#             }), 400
        
#         start_time = time.time()
        
#         # Check cache first
#         cached_result = query_cache.get(user_query)
#         if cached_result:
#             cached_result["cached"] = True
#             cached_result["time_ms"] = int((time.time() - start_time) * 1000)
#             cached_result["cache_stats"] = query_cache.get_stats()
#             return jsonify(cached_result)
        
#         query_lower = user_query.lower()
        
#         # Detect query type
#         is_document_query = any(word in query_lower for word in 
#             ['resume', 'cv', 'document', 'find', 'mentioning', 'skills'])
#         is_database_query = any(word in query_lower for word in 
#             ['employee', 'salary', 'department', 'count', 'average', 'top', 'highest', 
#              'paid', 'hired', 'list', 'show', 'reports', 'who', 'manager'])
        
#         db_state = get_db_state()
#         has_database = db_state['connected'] and db_state['schema']
#         has_documents = documents_index and len(documents_index) > 0
        
#         # Route to appropriate handler
#         if is_document_query and has_documents and not is_database_query:
#             raw_result = handle_document_query(user_query, current_schema, documents_index)
            
#             doc_hits = []
#             if raw_result and "documents" in raw_result:
#                 for doc in raw_result["documents"]:
#                     doc_hits.append({
#                         "doc_name": doc.get("filename", "unknown"),
#                         "chunk_text": doc.get("chunk", ""),
#                         "score": doc.get("score", 0),
#                         "confidence": doc.get("confidence", 0),
#                         "matched_keywords": doc.get("matched_keywords", [])
#                     })
            
#             doc_hits.sort(key=lambda x: x["score"], reverse=True)
            
#             response = {
#                 "ok": True,
#                 "result": {
#                     "type": "document",
#                     "documents": {"hits": doc_hits, "total": len(doc_hits)},
#                     "db": {}
#                 },
#                 "time_ms": int((time.time() - start_time) * 1000),
#                 "cached": False,
#                 "cache_stats": query_cache.get_stats()
#             }
            
#         elif is_database_query and has_database:
#             sql_result = generate_sql_for_query(user_query, db_state['schema'])
            
#             if sql_result['sql']:
#                 db_results = execute_sql_query(sql_result['sql'], db_state['engine'])
                
#                 response = {
#                     "ok": True,
#                     "result": {
#                         "type": "database",
#                         "query_type": sql_result['query_type'],
#                         "sql_generated": sql_result['sql'],
#                         "database_results": db_results,
#                         "documents": {"hits": [], "total": 0}
#                     },
#                     "time_ms": int((time.time() - start_time) * 1000),
#                     "cached": False,
#                     "cache_stats": query_cache.get_stats()
#                 }
#             else:
#                 return jsonify({
#                     "ok": False,
#                     "error": "query_not_understood",
#                     "message": sql_result.get('error_message', "Could not generate SQL"),
#                     "cache_stats": query_cache.get_stats()
#                 }), 400
        
#         else:
#             if has_documents:
#                 raw_result = handle_document_query(user_query, current_schema, documents_index)
                
#                 doc_hits = []
#                 if raw_result and "documents" in raw_result:
#                     for doc in raw_result["documents"]:
#                         doc_hits.append({
#                             "doc_name": doc.get("filename", "unknown"),
#                             "chunk_text": doc.get("chunk", ""),
#                             "score": doc.get("score", 0)
#                         })
                
#                 doc_hits.sort(key=lambda x: x["score"], reverse=True)
                
#                 response = {
#                     "ok": True,
#                     "result": {
#                         "type": "document",
#                         "documents": {"hits": doc_hits, "total": len(doc_hits)},
#                         "db": {}
#                     },
#                     "time_ms": int((time.time() - start_time) * 1000),
#                     "cached": False,
#                     "cache_stats": query_cache.get_stats()
#                 }
#             else:
#                 return jsonify({
#                     "ok": False,
#                     "error": "no_data_source",
#                     "message": "Please connect to database or upload documents first",
#                     "cache_stats": query_cache.get_stats()
#                 }), 400
        
#         # Cache the response
#         query_cache.set(user_query, response.copy())
        
#         # Add to history (keep last 50)
#         query_history.append({
#             "query": user_query,
#             "timestamp": datetime.now().isoformat(),
#             "type": response["result"]["type"],
#             "time_ms": response["time_ms"]
#         })
#         if len(query_history) > 50:
#             query_history.pop(0)
        
#         return jsonify(response)
        
#     except Exception as e:
#         logger.error(f"Query error: {e}", exc_info=True)
#         return jsonify({
#             "ok": False, 
#             "error": "execution_error",
#             "message": str(e),
#             "cache_stats": query_cache.get_stats()
#         }), 500

# @query_bp.route("/query/history", methods=["GET"])
# def get_query_history():
#     """Get query history for caching demo"""
#     return jsonify({
#         "ok": True,
#         "history": query_history[-20:],  # Last 20 queries
#         "total": len(query_history)
#     })

# @query_bp.route("/query/stats", methods=["GET"])
# def get_query_stats():
#     """Get cache and performance statistics"""
#     return jsonify({
#         "ok": True,
#         "cache": query_cache.get_stats(),
#         "queries_total": len(query_history),
#         "recent_queries": query_history[-5:]
#     })

# def generate_sql_for_query(user_query, schema):
#     """Generate SQL from natural language"""
#     query_lower = (user_query or "").strip().lower()
#     debug_info = {"query": user_query, "available_tables": list(schema.keys())}
#     sql = None
#     query_type = "select"
#     error_message = None
#     current_year = datetime.now().year

#     def find_table_by_keywords(keys):
#         for t in schema.keys():
#             for k in keys:
#                 if k in t.lower():
#                     return t
#         return None

#     employee_table = find_table_by_keywords(['employee', 'staff', 'personnel']) or list(schema.keys())[0]
#     department_table = find_table_by_keywords(['department', 'dept', 'division'])

#     emp_cols = schema.get(employee_table, [])
#     dept_cols = schema.get(department_table, []) if department_table else []

#     def find_column(cols, patterns):
#         cols_lower = [c.lower() for c in cols]
#         for pattern in patterns:
#             for i, col_lower in enumerate(cols_lower):
#                 if pattern in col_lower:
#                     return cols[i]
#         return None

#     name_col = find_column(emp_cols, ['full_name', 'employee_name', 'name'])
#     dept_col = find_column(emp_cols, ['dept_id', 'department', 'division'])
#     salary_col = find_column(emp_cols, ['salary', 'compensation', 'pay'])
    
#     # Query patterns
#     if 'how many' in query_lower or 'count' in query_lower:
#         query_type = "count"
#         sql = f"SELECT COUNT(*) AS total FROM {employee_table}"
    
#     elif 'average salary' in query_lower:
#         query_type = "average"
#         if salary_col and dept_col:
#             sql = f"SELECT {dept_col}, AVG({salary_col}) as avg_salary FROM {employee_table} GROUP BY {dept_col}"
#         elif salary_col:
#             sql = f"SELECT AVG({salary_col}) as avg_salary FROM {employee_table}"
    
#     elif 'top' in query_lower and 'highest' in query_lower:
#         query_type = "top_n"
#         if salary_col and name_col:
#             sql = f"SELECT {name_col}, {salary_col} FROM {employee_table} ORDER BY {salary_col} DESC LIMIT 5"
    
#     else:
#         query_type = "select"
#         select_cols = [c for c in [name_col, dept_col, salary_col] if c]
#         if select_cols:
#             sql = f"SELECT {', '.join(select_cols)} FROM {employee_table} LIMIT 50"
#         else:
#             sql = f"SELECT * FROM {employee_table} LIMIT 50"

#     return {
#         'sql': sql,
#         'query_type': query_type,
#         'debug_info': debug_info,
#         'error_message': error_message
#     }

# def execute_sql_query(sql, engine):
#     """Execute SQL with error handling"""
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(sql))
#             rows = result.fetchall()
            
#             data = []
#             if rows:
#                 columns = list(result.keys())
#                 for row in rows:
#                     row_dict = {}
#                     for key in columns:
#                         value = row._mapping[key]
#                         if hasattr(value, 'isoformat'):
#                             row_dict[key] = value.isoformat()
#                         else:
#                             row_dict[key] = value
#                     data.append(row_dict)
            
#             return {
#                 'success': True,
#                 'data': data,
#                 'row_count': len(data)
#             }
    
#     except Exception as e:
#         logger.error(f"SQL execution error: {e}")
#         return {
#             'success': False,
#             'error': str(e),
#             'data': []
#         }

from flask import Blueprint, request, jsonify
import time
import logging
from sqlalchemy import text
from datetime import datetime
import re
import hashlib
from collections import OrderedDict
import asyncio
from concurrent.futures import ThreadPoolExecutor
import bleach

logger = logging.getLogger(__name__)

try:
    from services.query_engine import handle_query as handle_document_query
    from routes.schema import current_schema, get_db_state
    from routes.ingestion import documents_index
    IMPORTS_OK = True
except ImportError as e:
    print(f"Import error: {e}")
    IMPORTS_OK = False

query_bp = Blueprint("query", __name__)

# Thread pool for async operations
executor = ThreadPoolExecutor(max_workers=10)

# SQL Injection Prevention - Dangerous keywords/patterns
DANGEROUS_SQL_PATTERNS = [
    r'\b(DROP|DELETE|TRUNCATE|ALTER|CREATE|GRANT|REVOKE)\b',
    r';.*--',  # Comment injection
    r';\s*DROP',  # Drop table injection
    r'UNION.*SELECT',  # Union injection
    r'INSERT\s+INTO',
    r'UPDATE.*SET',
    r'EXEC(\s|\()',  # Execute injection
    r'xp_.*\(',  # SQL Server extended procedures
    r'sp_.*\(',  # SQL Server system procedures
]

def sanitize_query_input(query: str) -> str:
    """Sanitize user input to prevent SQL injection"""
    if not query:
        return ""
    
    # Remove any HTML/script tags
    query = bleach.clean(query, tags=[], strip=True)
    
    # Check for dangerous SQL patterns
    for pattern in DANGEROUS_SQL_PATTERNS:
        if re.search(pattern, query, re.IGNORECASE):
            raise ValueError(f"Potentially dangerous SQL pattern detected: {pattern}")
    
    # Limit query length
    if len(query) > 500:
        raise ValueError("Query too long (max 500 characters)")
    
    return query.strip()

def validate_generated_sql(sql: str) -> bool:
    """Validate generated SQL before execution"""
    if not sql:
        return False
    
    sql_upper = sql.upper()
    
    # Only allow SELECT statements
    if not sql_upper.strip().startswith('SELECT'):
        logger.warning(f"Blocked non-SELECT query: {sql}")
        return False
    
    # Block dangerous operations
    dangerous_ops = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE', 'EXEC']
    for op in dangerous_ops:
        if op in sql_upper:
            logger.warning(f"Blocked query with {op}: {sql}")
            return False
    
    return True

class QueryCache:
    """Enhanced query cache with TTL and metrics"""
    def __init__(self, max_size=1000, ttl_seconds=300):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.hits = 0
        self.misses = 0
        self.total_queries = 0
    
    def _make_key(self, query):
        return hashlib.md5(query.lower().encode()).hexdigest()
    
    def get(self, query):
        self.total_queries += 1
        key = self._make_key(query)
        
        if key in self.cache:
            cached_data, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl_seconds:
                self.hits += 1
                self.cache.move_to_end(key)
                return cached_data
            else:
                del self.cache[key]
        
        self.misses += 1
        return None
    
    def set(self, query, data):
        key = self._make_key(query)
        
        if len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
        
        self.cache[key] = (data, time.time())
    
    def clear(self):
        """Clear all cached queries"""
        self.cache.clear()
    
    def invalidate(self, query):
        """Invalidate specific query from cache"""
        key = self._make_key(query)
        if key in self.cache:
            del self.cache[key]
    
    def get_stats(self):
        hit_rate = (self.hits / self.total_queries * 100) if self.total_queries > 0 else 0
        return {
            "total_queries": self.total_queries,
            "cache_hits": self.hits,
            "cache_misses": self.misses,
            "hit_rate_percent": round(hit_rate, 2),
            "cache_size": len(self.cache),
            "max_size": self.max_size
        }

# Global cache instance
query_cache = QueryCache(max_size=1000, ttl_seconds=300)
query_history = []

def process_query_async(user_query: str, db_state: dict, has_documents: bool):
    """Process query asynchronously"""
    try:
        query_lower = user_query.lower()
        
        is_document_query = any(word in query_lower for word in 
            ['resume', 'cv', 'document', 'find', 'mentioning', 'skills'])
        is_database_query = any(word in query_lower for word in 
            ['employee', 'salary', 'department', 'count', 'average', 'top', 'highest', 
             'paid', 'hired', 'list', 'show', 'reports', 'who', 'manager'])
        
        has_database = db_state['connected'] and db_state['schema']
        
        # Route to appropriate handler
        if is_document_query and has_documents and not is_database_query:
            return handle_document_search(user_query)
        elif is_database_query and has_database:
            return handle_database_search(user_query, db_state)
        elif has_documents:
            return handle_document_search(user_query)
        else:
            raise ValueError("No data source available. Please connect database or upload documents.")
            
    except Exception as e:
        logger.error(f"Query processing error: {e}", exc_info=True)
        raise

def handle_document_search(user_query: str):
    """Handle document-only search"""
    raw_result = handle_document_query(user_query, current_schema, documents_index)
    
    doc_hits = []
    if raw_result and "documents" in raw_result:
        for doc in raw_result["documents"]:
            doc_hits.append({
                "doc_name": doc.get("filename", "unknown"),
                "chunk_text": doc.get("chunk", ""),
                "score": doc.get("score", 0),
                "confidence": doc.get("confidence", 0),
                "matched_keywords": doc.get("matched_keywords", [])
            })
    
    doc_hits.sort(key=lambda x: x["score"], reverse=True)
    
    return {
        "type": "document",
        "documents": {"hits": doc_hits, "total": len(doc_hits)},
        "db": {}
    }

def handle_database_search(user_query: str, db_state: dict):
    """Handle database-only search with SQL injection prevention"""
    sql_result = generate_sql_for_query(user_query, db_state['schema'])
    
    if not sql_result['sql']:
        raise ValueError(sql_result.get('error_message', "Could not generate SQL"))
    
    # Validate SQL before execution
    if not validate_generated_sql(sql_result['sql']):
        raise ValueError("Generated SQL failed security validation")
    
    db_results = execute_sql_query(sql_result['sql'], db_state['engine'])
    
    return {
        "type": "database",
        "query_type": sql_result['query_type'],
        "sql_generated": sql_result['sql'],
        "database_results": db_results,
        "documents": {"hits": [], "total": 0}
    }

@query_bp.route("/query", methods=["POST"])
def query():
    """Process natural language queries with caching, security, and async support"""
    if not IMPORTS_OK:
        return jsonify({
            "ok": False, 
            "error": "system_error",
            "message": "Required modules not available"
        }), 500
    
    try:
        data = request.json
        if not data:
            return jsonify({
                "ok": False,
                "error": "no_data", 
                "message": "No JSON data provided"
            }), 400
        
        user_query = data.get("query", "").strip()
        if not user_query:
            return jsonify({
                "ok": False,
                "error": "empty_query",
                "message": "Query cannot be empty"
            }), 400
        
        # Sanitize input to prevent SQL injection
        try:
            user_query = sanitize_query_input(user_query)
        except ValueError as e:
            return jsonify({
                "ok": False,
                "error": "invalid_query",
                "message": str(e)
            }), 400
        
        start_time = time.time()
        
        # Check cache first
        cached_result = query_cache.get(user_query)
        if cached_result:
            cached_result["cached"] = True
            cached_result["time_ms"] = int((time.time() - start_time) * 1000)
            cached_result["cache_stats"] = query_cache.get_stats()
            return jsonify(cached_result)
        
        db_state = get_db_state()
        has_documents = documents_index and len(documents_index) > 0
        
        # Process query asynchronously
        try:
            future = executor.submit(process_query_async, user_query, db_state, has_documents)
            result = future.result(timeout=10)  # 10 second timeout
            
            response = {
                "ok": True,
                "result": result,
                "time_ms": int((time.time() - start_time) * 1000),
                "cached": False,
                "cache_stats": query_cache.get_stats()
            }
            
        except TimeoutError:
            return jsonify({
                "ok": False,
                "error": "timeout",
                "message": "Query processing timeout (10s limit)"
            }), 408
        except ValueError as e:
            return jsonify({
                "ok": False,
                "error": "processing_error",
                "message": str(e),
                "cache_stats": query_cache.get_stats()
            }), 400
        
        # Cache the response
        query_cache.set(user_query, response.copy())
        
        # Add to history (keep last 50)
        query_history.append({
            "query": user_query,
            "timestamp": datetime.now().isoformat(),
            "type": response["result"]["type"],
            "time_ms": response["time_ms"]
        })
        if len(query_history) > 50:
            query_history.pop(0)
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Query error: {e}", exc_info=True)
        return jsonify({
            "ok": False, 
            "error": "execution_error",
            "message": str(e),
            "cache_stats": query_cache.get_stats()
        }), 500

@query_bp.route("/query/history", methods=["GET"])
def get_query_history():
    """Get query history for caching demo"""
    return jsonify({
        "ok": True,
        "history": query_history[-20:],
        "total": len(query_history)
    })

@query_bp.route("/query/stats", methods=["GET"])
def get_query_stats():
    """Get cache and performance statistics"""
    return jsonify({
        "ok": True,
        "cache": query_cache.get_stats(),
        "queries_total": len(query_history),
        "recent_queries": query_history[-5:]
    })

@query_bp.route("/query/cache/clear", methods=["POST"])
def clear_cache():
    """Clear query cache"""
    query_cache.clear()
    return jsonify({
        "ok": True,
        "message": "Cache cleared successfully"
    })

def generate_sql_for_query(user_query, schema):
    """Generate SQL from natural language with pagination support"""
    query_lower = (user_query or "").strip().lower()
    debug_info = {"query": user_query, "available_tables": list(schema.keys())}
    sql = None
    query_type = "select"
    error_message = None
    current_year = datetime.now().year

    def find_table_by_keywords(keys):
        for t in schema.keys():
            for k in keys:
                if k in t.lower():
                    return t
        return None

    employee_table = find_table_by_keywords(['employee', 'staff', 'personnel']) or list(schema.keys())[0]
    department_table = find_table_by_keywords(['department', 'dept', 'division'])

    emp_cols = schema.get(employee_table, [])
    dept_cols = schema.get(department_table, []) if department_table else []

    def find_column(cols, patterns):
        cols_lower = [c.lower() for c in cols]
        for pattern in patterns:
            for i, col_lower in enumerate(cols_lower):
                if pattern in col_lower:
                    return cols[i]
        return None

    name_col = find_column(emp_cols, ['full_name', 'employee_name', 'name'])
    dept_col = find_column(emp_cols, ['dept_id', 'department', 'division'])
    salary_col = find_column(emp_cols, ['salary', 'compensation', 'pay'])
    join_date_col = find_column(emp_cols, ['join_date', 'hire_date', 'hired_on', 'start_date'])
    
    # Query patterns with result limits
    if 'how many' in query_lower or 'count' in query_lower:
        query_type = "count"
        sql = f"SELECT COUNT(*) AS total FROM {employee_table}"
    
    elif 'average salary' in query_lower:
        query_type = "average"
        if salary_col and dept_col:
            sql = f"SELECT {dept_col}, AVG({salary_col}) as avg_salary FROM {employee_table} GROUP BY {dept_col} LIMIT 100"
        elif salary_col:
            sql = f"SELECT AVG({salary_col}) as avg_salary FROM {employee_table}"
    
    elif 'top' in query_lower and 'highest' in query_lower:
        query_type = "top_n"
        if salary_col and name_col:
            sql = f"SELECT {name_col}, {salary_col} FROM {employee_table} ORDER BY {salary_col} DESC LIMIT 5"
    
    elif 'hired this year' in query_lower and join_date_col:
        query_type = "hired_this_year"
        sql = f"SELECT {name_col}, {join_date_col} FROM {employee_table} WHERE YEAR({join_date_col}) = {current_year} LIMIT 100"
    
    else:
        query_type = "select"
        select_cols = [c for c in [name_col, dept_col, salary_col] if c]
        if select_cols:
            sql = f"SELECT {', '.join(select_cols)} FROM {employee_table} LIMIT 50"
        else:
            sql = f"SELECT * FROM {employee_table} LIMIT 50"

    return {
        'sql': sql,
        'query_type': query_type,
        'debug_info': debug_info,
        'error_message': error_message
    }

def execute_sql_query(sql, engine):
    """Execute SQL with error handling and parameterization"""
    try:
        # Use parameterized queries where possible (SQLAlchemy handles this)
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = result.fetchall()
            
            data = []
            if rows:
                columns = list(result.keys())
                for row in rows:
                    row_dict = {}
                    for key in columns:
                        value = row._mapping[key]
                        if hasattr(value, 'isoformat'):
                            row_dict[key] = value.isoformat()
                        else:
                            row_dict[key] = value
                    data.append(row_dict)
            
            return {
                'success': True,
                'data': data,
                'row_count': len(data)
            }
    
    except Exception as e:
        logger.error(f"SQL execution error: {e}")
        return {
            'success': False,
            'error': str(e),
            'data': []
        }