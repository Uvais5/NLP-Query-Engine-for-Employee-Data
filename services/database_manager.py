# """
# Database Manager - Shared state between routes
# Maintains connection info and schema across the application
# """
# import logging
# from sqlalchemy import create_engine, text, inspect
# from typing import Dict, Any, Optional

# logger = logging.getLogger(__name__)

# class DatabaseManager:
#     """Singleton class to manage database connections and schema across routes"""
    
#     _instance = None
#     _initialized = False
    
#     def __new__(cls):
#         if cls._instance is None:
#             cls._instance = super(DatabaseManager, cls).__new__(cls)
#         return cls._instance
    
#     def __init__(self):
#         if not self._initialized:
#             self.engine = None
#             self.connection_string = None
#             self.db_type = None
#             self.schema_info = None
#             self.current_schema = None  # Table -> columns mapping
#             self.is_connected = False
#             DatabaseManager._initialized = True
    
#     def connect(self, connection_string: str) -> Dict[str, Any]:
#         """Connect to database and discover schema"""
#         try:
#             logger.info(f"DatabaseManager: Connecting to {connection_string}")
            
#             # Normalize connection string
#             normalized_conn = self._normalize_connection_string(connection_string)
            
#             # Create engine
#             self.engine = create_engine(normalized_conn, echo=False)
            
#             # Test connection
#             with self.engine.connect() as conn:
#                 result = conn.execute(text("SELECT 1 as test"))
#                 test_result = result.fetchone()
#                 logger.info(f"Connection test successful: {test_result}")
            
#             # Store connection info
#             self.connection_string = normalized_conn
#             self.db_type = self._get_db_type(normalized_conn)
            
#             # Discover schema
#             schema_info = self._discover_schema()
            
#             self.schema_info = schema_info
#             self.current_schema = {
#                 table_name: [col['name'] for col in table_info['columns']]
#                 for table_name, table_info in schema_info['tables'].items()
#             }
#             self.is_connected = True
            
#             logger.info(f"DatabaseManager: Connected successfully. Schema: {list(self.current_schema.keys())}")
            
#             return schema_info
            
#         except Exception as e:
#             logger.error(f"DatabaseManager connection error: {e}")
#             self.disconnect()
#             raise e
    
#     def _normalize_connection_string(self, connection_string: str) -> str:
#         """Normalize different connection string formats"""
#         conn_str = connection_string.strip()
        
#         if conn_str.startswith('localhost/'):
#             database = conn_str.split('/', 1)[1]
#             return f"mysql+pymysql://root:@localhost:3306/{database}"
#         elif conn_str.startswith('mysql://'):
#             return conn_str.replace('mysql://', 'mysql+pymysql://')
#         else:
#             return conn_str
    
#     def _get_db_type(self, connection_string: str) -> str:
#         """Determine database type from connection string"""
#         if 'mysql' in connection_string:
#             return 'mysql'
#         elif 'postgresql' in connection_string or 'postgres' in connection_string:
#             return 'postgresql'
#         elif 'sqlite' in connection_string:
#             return 'sqlite'
#         else:
#             return 'unknown'
    
#     def _discover_schema(self) -> Dict[str, Any]:
#         """Discover database schema"""
#         try:
#             inspector = inspect(self.engine)
#             table_names = inspector.get_table_names()
            
#             schema_info = {
#                 'connection_string': self.connection_string,
#                 'database_type': self.db_type,
#                 'tables': {},
#                 'summary': {
#                     'total_tables': len(table_names),
#                     'employee_tables': [],
#                     'department_tables': []
#                 }
#             }
            
#             for table_name in table_names:
#                 columns = inspector.get_columns(table_name)
                
#                 # Process columns
#                 processed_columns = []
#                 for col in columns:
#                     processed_columns.append({
#                         'name': col['name'],
#                         'type': str(col['type']),
#                         'nullable': col['nullable'],
#                         'default': col.get('default')
#                     })
                
#                 # Classify table purpose
#                 purpose = self._classify_table_purpose(table_name, processed_columns)
                
#                 # Get semantic mapping
#                 semantic_mapping = self._get_semantic_mapping(processed_columns)
                
#                 # Get sample data
#                 sample_data = self._get_sample_data(table_name, 2)
#                 row_count = self._get_row_count(table_name)
                
#                 schema_info['tables'][table_name] = {
#                     'name': table_name,
#                     'purpose': purpose,
#                     'columns': processed_columns,
#                     'semantic_mapping': semantic_mapping,
#                     'sample_data': sample_data,
#                     'row_count': row_count,
#                     'column_count': len(processed_columns)
#                 }
                
#                 # Add to summary
#                 if purpose == 'employee':
#                     schema_info['summary']['employee_tables'].append(table_name)
#                 elif purpose == 'department':
#                     schema_info['summary']['department_tables'].append(table_name)
            
#             schema_info['summary']['total_columns'] = sum(
#                 len(table['columns']) for table in schema_info['tables'].values()
#             )
            
#             return schema_info
            
#         except Exception as e:
#             logger.error(f"Schema discovery error: {e}")
#             raise e
    
#     def _classify_table_purpose(self, table_name: str, columns: list) -> str:
#         """Classify what type of data this table contains"""
#         table_lower = table_name.lower()
#         column_names = [col['name'].lower() for col in columns]
        
#         # Direct name matching
#         if any(pattern in table_lower for pattern in ['employee', 'staff', 'personnel']):
#             return 'employee'
#         elif any(pattern in table_lower for pattern in ['department', 'dept', 'division']):
#             return 'department'
        
#         # Column-based inference
#         employee_indicators = ['salary', 'wage', 'pay', 'compensation', 'position', 'title', 'join_date', 'hire']
#         department_indicators = ['dept_name', 'department_name', 'manager']
        
#         employee_score = sum(1 for col in column_names if any(ind in col for ind in employee_indicators))
#         department_score = sum(1 for col in column_names if any(ind in col for ind in department_indicators))
        
#         if employee_score > department_score:
#             return 'employee'
#         elif department_score > 0:
#             return 'department'
#         else:
#             return 'other'
    
#     def _get_semantic_mapping(self, columns: list) -> Dict[str, str]:
#         """Map column names to semantic meanings"""
#         column_names = [col['name'].lower() for col in columns]
#         mapping = {}
        
#         # Define patterns for semantic mapping
#         patterns = {
#             'id': ['id', 'emp_id', 'employee_id', 'person_id', 'staff_id', 'dept_id'],
#             'name': ['name', 'full_name', 'employee_name', 'first_name', 'last_name'],
#             'salary': ['salary', 'annual_salary', 'compensation', 'pay', 'pay_rate', 'wage'],
#             'department': ['department', 'dept', 'division', 'dept_name', 'department_name'],
#             'position': ['position', 'title', 'role', 'job_title', 'designation'],
#             'date': ['join_date', 'hire_date', 'hired_on', 'start_date', 'employment_date']
#         }
        
#         for semantic_name, pattern_list in patterns.items():
#             for col_name in column_names:
#                 for pattern in pattern_list:
#                     if pattern in col_name:
#                         # Find original case column name
#                         original_name = next(col['name'] for col in columns if col['name'].lower() == col_name)
#                         mapping[semantic_name] = original_name
#                         break
#                 if semantic_name in mapping:
#                     break
        
#         return mapping
    
#     def _get_sample_data(self, table_name: str, limit: int = 2) -> list:
#         """Get sample data from table"""
#         try:
#             with self.engine.connect() as conn:
#                 result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT {limit}"))
                
#                 sample_data = []
#                 for row in result:
#                     row_dict = {}
#                     for key, value in row._mapping.items():
#                         if isinstance(value, (str, int, float, bool, type(None))):
#                             row_dict[key] = value
#                         else:
#                             row_dict[key] = str(value)
#                     sample_data.append(row_dict)
                
#                 return sample_data
#         except Exception as e:
#             logger.error(f"Error getting sample data from {table_name}: {e}")
#             return []
    
#     def _get_row_count(self, table_name: str) -> int:
#         """Get total row count for table"""
#         try:
#             with self.engine.connect() as conn:
#                 result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
#                 return result.scalar()
#         except Exception as e:
#             logger.error(f"Error getting row count for {table_name}: {e}")
#             return 0
    
#     def execute_sql(self, sql: str) -> Dict[str, Any]:
#         """Execute SQL query against connected database"""
#         try:
#             if not self.is_connected or not self.engine:
#                 raise Exception("No database connection available")
            
#             logger.info(f"Executing SQL: {sql}")
            
#             with self.engine.connect() as conn:
#                 result = conn.execute(text(sql))
                
#                 # Fetch all results
#                 rows = result.fetchall()
                
#                 # Convert to list of dictionaries
#                 data = []
#                 if rows:
#                     columns = result.keys()
#                     for row in rows:
#                         row_dict = {}
#                         for i, value in enumerate(row):
#                             col_name = columns[i]
#                             if isinstance(value, (str, int, float, bool, type(None))):
#                                 row_dict[col_name] = value
#                             else:
#                                 row_dict[col_name] = str(value)
#                         data.append(row_dict)
                
#                 return {
#                     'success': True,
#                     'data': data,
#                     'row_count': len(data)
#                 }
                
#         except Exception as e:
#             logger.error(f"SQL execution error: {e}")
#             return {
#                 'success': False,
#                 'error': str(e),
#                 'data': []
#             }
    
#     def get_schema_info(self) -> Optional[Dict[str, Any]]:
#         """Get current schema information"""
#         return self.schema_info
    
#     def get_current_schema(self) -> Optional[Dict[str, list]]:
#         """Get current schema (table -> columns mapping)"""
#         return self.current_schema
    
#     def is_database_connected(self) -> bool:
#         """Check if database is connected"""
#         return self.is_connected
    
#     def disconnect(self):
#         """Disconnect from database"""
#         try:
#             if self.engine:
#                 self.engine.dispose()
            
#             self.engine = None
#             self.connection_string = None
#             self.db_type = None
#             self.schema_info = None
#             self.current_schema = None
#             self.is_connected = False
            
#             logger.info("DatabaseManager: Disconnected")
            
#         except Exception as e:
#             logger.error(f"Error during disconnect: {e}")

# # Global instance
# db_manager = DatabaseManager()