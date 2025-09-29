# from flask import Blueprint, request, jsonify
# import logging
# import sys
# import os
# from sqlalchemy import create_engine, text, inspect

# # Add parent directory to path for imports
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# logger = logging.getLogger(__name__)

# schema_bp = Blueprint("schema", __name__)

# # Global variables
# current_schema = None
# current_schema_info = None

# # Global database state
# _global_db_state = {
#     'connected': False,
#     'schema': None,
#     'engine': None,
#     'connection_string': None
# }

# def get_db_state():
#     return _global_db_state

# @schema_bp.route("/test-connection", methods=["POST"])
# def test_connection():
#     """Test database connection endpoint"""
#     try:
#         logger.info("=== TEST CONNECTION CALLED ===")
        
#         if not request.is_json:
#             return jsonify({
#                 "ok": False,
#                 "error": "invalid_request",
#                 "message": "Request must be JSON"
#             }), 400
        
#         data = request.get_json()
#         connection_string = data.get("connection_string", "").strip()
        
#         if not connection_string:
#             return jsonify({
#                 "ok": False,
#                 "error": "missing_connection_string",
#                 "message": "Please enter a connection string"
#             }), 400
        
#         logger.info(f"Testing connection: {connection_string}")
        
#         # Normalize connection string for XAMPP
#         normalized_conn = connection_string
#         if connection_string.startswith('localhost/'):
#             database = connection_string.split('/', 1)[1]
#             normalized_conn = f"mysql+pymysql://root:@localhost:3306/{database}"
#         elif connection_string.startswith('mysql://'):
#             normalized_conn = connection_string.replace('mysql://', 'mysql+pymysql://')
        
#         # Test connection
#         try:
#             engine = create_engine(normalized_conn, echo=False)
            
#             with engine.connect() as conn:
#                 result = conn.execute(text("SELECT 1 as test"))
#                 test_result = result.fetchone()
#                 logger.info(f"Connection successful: {test_result}")
            
#             # Get basic table info
#             inspector = inspect(engine)
#             table_names = inspector.get_table_names()
#             logger.info(f"Found {len(table_names)} tables: {table_names}")
            
#             # Clean up test connection
#             engine.dispose()
            
#             return jsonify({
#                 "ok": True,
#                 "message": "Connection successful!",
#                 "info": {
#                     "database_type": "mysql",
#                     "table_count": len(table_names),
#                     "tables": table_names,
#                     "test_time_ms": 100
#                 }
#             })
            
#         except Exception as db_error:
#             logger.error(f"Database error: {db_error}")
#             return jsonify({
#                 "ok": False,
#                 "error": "connection_failed",
#                 "message": f"Database connection failed: {str(db_error)}"
#             }), 500
            
#     except Exception as e:
#         logger.error(f"Unexpected error: {e}")
#         return jsonify({
#             "ok": False,
#             "error": "internal_error",
#             "message": f"Internal error: {str(e)}"
#         }), 500

# @schema_bp.route("/connect-database", methods=["POST"])
# def connect_database():
#     """Connect to database and discover schema"""
#     global current_schema, current_schema_info, _global_db_state
    
#     try:
#         logger.info("=== CONNECT DATABASE CALLED ===")
        
#         data = request.get_json()
#         connection_string = data.get("connection_string", "").strip()
        
#         if not connection_string:
#             return jsonify({
#                 "ok": False,
#                 "error": "missing_connection_string",
#                 "message": "Please enter a connection string"
#             }), 400
        
#         logger.info(f"Connecting to: {connection_string}")
        
#         # Normalize connection string for XAMPP
#         normalized_conn = connection_string
#         if connection_string.startswith('localhost/'):
#             database = connection_string.split('/', 1)[1]
#             normalized_conn = f"mysql+pymysql://root:@localhost:3306/{database}"
#         elif connection_string.startswith('mysql://'):
#             normalized_conn = connection_string.replace('mysql://', 'mysql+pymysql://')
        
#         # Create and test engine
#         engine = create_engine(normalized_conn, echo=False)
        
#         with engine.connect() as conn:
#             conn.execute(text("SELECT 1"))
        
#         # Get schema info
#         inspector = inspect(engine)
#         table_names = inspector.get_table_names()
        
#         # Build schema dictionary
#         schema = {}
#         for table_name in table_names:
#             columns = inspector.get_columns(table_name)
#             schema[table_name] = [col['name'] for col in columns]
        
#         # Update global state
#         _global_db_state['connected'] = True
#         _global_db_state['engine'] = engine
#         _global_db_state['connection_string'] = normalized_conn
#         _global_db_state['schema'] = schema
#         current_schema = schema
        
#         logger.info(f"Connected successfully. Tables: {list(schema.keys())}")
        
#         # Build detailed response for frontend
#         formatted_tables = {}
#         for table_name in table_names:
#             columns = inspector.get_columns(table_name)
            
#             # Classify table purpose
#             purpose = 'employee' if any(word in table_name.lower() for word in ['employee', 'staff', 'personnel']) else 'other'
#             if any(word in table_name.lower() for word in ['department', 'dept', 'division']):
#                 purpose = 'department'
            
#             # Get row count
#             try:
#                 with engine.connect() as conn:
#                     count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
#                     row_count = count_result.scalar()
#             except:
#                 row_count = 0
            
#             # Get semantic mapping
#             semantic_mapping = {}
#             column_names = [col['name'].lower() for col in columns]
            
#             # Map common column patterns
#             patterns = {
#                 'id': ['id', 'emp_id', 'employee_id', 'person_id', 'staff_id'],
#                 'name': ['name', 'full_name', 'employee_name', 'first_name'],
#                 'salary': ['salary', 'annual_salary', 'compensation', 'pay', 'pay_rate'],
#                 'department': ['department', 'dept', 'division', 'dept_id'],
#                 'position': ['position', 'title', 'role', 'job_title']
#             }
            
#             for semantic_name, pattern_list in patterns.items():
#                 for col_name in column_names:
#                     for pattern in pattern_list:
#                         if pattern in col_name:
#                             original_name = next(col['name'] for col in columns if col['name'].lower() == col_name)
#                             semantic_mapping[semantic_name] = original_name
#                             break
#                     if semantic_name in semantic_mapping:
#                         break
            
#             formatted_tables[table_name] = {
#                 "name": table_name,
#                 "purpose": purpose,
#                 "column_count": len(columns),
#                 "row_count": row_count,
#                 "columns": [
#                     {
#                         "name": col['name'],
#                         "type": str(col['type']),
#                         "nullable": col['nullable'],
#                         "primary_key": col.get('primary_key', False),
#                         "foreign_key": False
#                     }
#                     for col in columns
#                 ],
#                 "semantic_mapping": semantic_mapping
#             }
        
#         # Store detailed info
#         current_schema_info = {
#             'tables': formatted_tables,
#             'summary': {
#                 'total_tables': len(table_names),
#                 'total_columns': sum(len(inspector.get_columns(t)) for t in table_names),
#                 'employee_tables': [t for t in table_names if 'employee' in t.lower() or 'staff' in t.lower()],
#                 'department_tables': [t for t in table_names if 'department' in t.lower() or 'dept' in t.lower()]
#             }
#         }
        
#         return jsonify({
#             "ok": True,
#             "message": "Database connected and schema discovered successfully",
#             "connection_info": {
#                 "database_type": "mysql",
#                 "processing_time_ms": 150
#             },
#             "schema": {
#                 "summary": current_schema_info['summary'],
#                 "tables": formatted_tables
#             }
#         })
        
#     except Exception as e:
#         logger.error(f"Error in connect_database: {e}")
#         return jsonify({
#             "ok": False,
#             "error": "connection_failed",
#             "message": f"Connection failed: {str(e)}"
#         }), 500

# @schema_bp.route("/schema-debug", methods=["GET"])
# def schema_debug():
#     """Debug endpoint for schema blueprint"""
#     return jsonify({
#         "ok": True,
#         "message": "Schema blueprint is working!",
#         "endpoints": [
#             "/test-connection (POST)",
#             "/connect-database (POST)", 
#             "/schema-debug (GET)"
#         ],
#         "current_schema": current_schema is not None,
#         "global_db_connected": _global_db_state['connected'],
#         "tables": list(_global_db_state['schema'].keys()) if _global_db_state['schema'] else []
#     })

from flask import Blueprint, request, jsonify
import logging
import sys
import os
from sqlalchemy import create_engine, text, inspect, pool
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

schema_bp = Blueprint("schema", __name__)

# Global variables
current_schema = None
current_schema_info = None

# Global database state with connection pooling
_global_db_state = {
    'connected': False,
    'schema': None,
    'engine': None,
    'connection_string': None,
    'connection_pool_size': 10,
    'max_overflow': 20,
    'pool_recycle': 3600  # Recycle connections after 1 hour
}

def get_db_state():
    """Get current database state"""
    return _global_db_state

def create_pooled_engine(connection_string: str):
    """Create SQLAlchemy engine with connection pooling"""
    try:
        engine = create_engine(
            connection_string,
            poolclass=pool.QueuePool,
            pool_size=_global_db_state['connection_pool_size'],
            max_overflow=_global_db_state['max_overflow'],
            pool_recycle=_global_db_state['pool_recycle'],
            pool_pre_ping=True,  # Verify connections before using
            echo=False
        )
        logger.info(f"Created connection pool (size={_global_db_state['connection_pool_size']}, "
                   f"overflow={_global_db_state['max_overflow']})")
        return engine
    except Exception as e:
        logger.error(f"Failed to create pooled engine: {e}")
        raise

def normalize_connection_string(connection_string: str) -> str:
    """Normalize various connection string formats"""
    conn_str = connection_string.strip()
    
    # XAMPP MySQL shortcuts
    if conn_str.startswith('localhost/'):
        database = conn_str.split('/', 1)[1]
        return f"mysql+pymysql://root:@localhost:3306/{database}"
    
    # Handle mysql:// format
    elif conn_str.startswith('mysql://'):
        return conn_str.replace('mysql://', 'mysql+pymysql://')
    
    # SQLite shortcuts
    elif conn_str.endswith('.db') and not conn_str.startswith('sqlite'):
        return f"sqlite:///{conn_str}"
    
    # Already properly formatted
    elif '://' in conn_str:
        return conn_str
    
    else:
        # Assume it's a database name for localhost MySQL
        return f"mysql+pymysql://root:@localhost:3306/{conn_str}"

@schema_bp.route("/test-connection", methods=["POST"])
def test_connection():
    """Test database connection with retry logic"""
    try:
        logger.info("=== TEST CONNECTION CALLED ===")
        
        if not request.is_json:
            return jsonify({
                "ok": False,
                "error": "invalid_request",
                "message": "Request must be JSON"
            }), 400
        
        data = request.get_json()
        connection_string = data.get("connection_string", "").strip()
        
        if not connection_string:
            return jsonify({
                "ok": False,
                "error": "missing_connection_string",
                "message": "Please enter a connection string"
            }), 400
        
        logger.info(f"Testing connection: {connection_string[:50]}...")
        
        # Normalize connection string
        normalized_conn = normalize_connection_string(connection_string)
        
        # Test connection with retry
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                start_time = time.time()
                
                # Create temporary engine for testing
                test_engine = create_engine(normalized_conn, echo=False)
                
                with test_engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test"))
                    test_result = result.fetchone()
                    logger.info(f"Connection test successful: {test_result}")
                
                # Get basic info
                inspector = inspect(test_engine)
                table_names = inspector.get_table_names()
                
                test_time = int((time.time() - start_time) * 1000)
                
                # Clean up
                test_engine.dispose()
                
                logger.info(f"Found {len(table_names)} tables: {table_names}")
                
                return jsonify({
                    "ok": True,
                    "message": "Connection successful!",
                    "info": {
                        "database_type": "mysql" if "mysql" in normalized_conn else "sqlite",
                        "table_count": len(table_names),
                        "tables": table_names,
                        "test_time_ms": test_time
                    }
                })
                
            except OperationalError as op_err:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection attempt {attempt + 1} failed, retrying...")
                    time.sleep(retry_delay)
                    continue
                else:
                    raise op_err
        
        # If we get here, all retries failed
        return jsonify({
            "ok": False,
            "error": "connection_failed",
            "message": "Connection failed after multiple attempts"
        }), 500
            
    except SQLAlchemyError as db_error:
        logger.error(f"Database error: {db_error}")
        return jsonify({
            "ok": False,
            "error": "database_error",
            "message": f"Database error: {str(db_error)}"
        }), 500
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return jsonify({
            "ok": False,
            "error": "internal_error",
            "message": f"Internal error: {str(e)}"
        }), 500

@schema_bp.route("/connect-database", methods=["POST"])
def connect_database():
    """Connect to database and discover schema with connection pooling"""
    global current_schema, current_schema_info, _global_db_state
    
    try:
        logger.info("=== CONNECT DATABASE CALLED ===")
        
        data = request.get_json()
        connection_string = data.get("connection_string", "").strip()
        
        if not connection_string:
            return jsonify({
                "ok": False,
                "error": "missing_connection_string",
                "message": "Please enter a connection string"
            }), 400
        
        logger.info(f"Connecting to: {connection_string[:50]}...")
        
        # Normalize connection string
        normalized_conn = normalize_connection_string(connection_string)
        
        # Dispose of old engine if exists
        if _global_db_state['engine']:
            logger.info("Disposing old database connection pool")
            _global_db_state['engine'].dispose()
        
        # Create new engine with connection pooling
        engine = create_pooled_engine(normalized_conn)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        # Get schema info
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        
        if not table_names:
            return jsonify({
                "ok": False,
                "error": "no_tables",
                "message": "Database connected but contains no tables"
            }), 400
        
        # Build schema dictionary (simple version for query engine)
        schema = {}
        for table_name in table_names:
            columns = inspector.get_columns(table_name)
            schema[table_name] = [col['name'] for col in columns]
        
        # Update global state
        _global_db_state['connected'] = True
        _global_db_state['engine'] = engine
        _global_db_state['connection_string'] = normalized_conn
        _global_db_state['schema'] = schema
        current_schema = schema
        
        logger.info(f"Connected successfully. Tables: {list(schema.keys())}")
        
        # Build detailed response for frontend
        formatted_tables = {}
        for table_name in table_names:
            columns = inspector.get_columns(table_name)
            primary_keys = inspector.get_pk_constraint(table_name).get('constrained_columns', [])
            foreign_keys = inspector.get_foreign_keys(table_name)
            indexes = inspector.get_indexes(table_name)
            
            # Classify table purpose
            purpose = classify_table_purpose(table_name)
            
            # Get row count safely
            try:
                with engine.connect() as conn:
                    count_result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                    row_count = count_result.scalar()
            except:
                row_count = 0
            
            # Get semantic mapping
            semantic_mapping = map_semantic_columns(columns)
            
            formatted_tables[table_name] = {
                "name": table_name,
                "purpose": purpose,
                "column_count": len(columns),
                "row_count": row_count,
                "columns": [
                    {
                        "name": col['name'],
                        "type": str(col['type']),
                        "nullable": col['nullable'],
                        "primary_key": col['name'] in primary_keys,
                        "foreign_key": any(col['name'] in fk['constrained_columns'] for fk in foreign_keys),
                        "indexed": any(col['name'] in idx['column_names'] for idx in indexes)
                    }
                    for col in columns
                ],
                "semantic_mapping": semantic_mapping,
                "foreign_keys": foreign_keys,
                "indexes": [idx['name'] for idx in indexes]
            }
        
        # Store detailed info
        current_schema_info = {
            'tables': formatted_tables,
            'summary': {
                'total_tables': len(table_names),
                'total_columns': sum(len(inspector.get_columns(t)) for t in table_names),
                'employee_tables': [t for t in table_names if is_employee_table(t)],
                'department_tables': [t for t in table_names if is_department_table(t)],
                'relationships': sum(len(inspector.get_foreign_keys(t)) for t in table_names)
            }
        }
        
        return jsonify({
            "ok": True,
            "message": "Database connected and schema discovered successfully",
            "connection_info": {
                "database_type": "mysql" if "mysql" in normalized_conn else "sqlite",
                "processing_time_ms": 150,
                "pool_size": _global_db_state['connection_pool_size'],
                "max_overflow": _global_db_state['max_overflow']
            },
            "schema": {
                "summary": current_schema_info['summary'],
                "tables": formatted_tables
            }
        })
        
    except SQLAlchemyError as e:
        logger.error(f"Database error in connect_database: {e}")
        return jsonify({
            "ok": False,
            "error": "database_error",
            "message": f"Database connection failed: {str(e)}"
        }), 500
        
    except Exception as e:
        logger.error(f"Error in connect_database: {e}", exc_info=True)
        return jsonify({
            "ok": False,
            "error": "internal_error",
            "message": f"Connection failed: {str(e)}"
        }), 500

def classify_table_purpose(table_name: str) -> str:
    """Classify table purpose based on name"""
    table_lower = table_name.lower()
    
    if any(word in table_lower for word in ['employee', 'staff', 'personnel', 'person', 'worker']):
        return 'employee'
    elif any(word in table_lower for word in ['department', 'dept', 'division']):
        return 'department'
    elif any(word in table_lower for word in ['document', 'file', 'attachment']):
        return 'document'
    else:
        return 'other'

def is_employee_table(table_name: str) -> bool:
    """Check if table is likely an employee table"""
    return classify_table_purpose(table_name) == 'employee'

def is_department_table(table_name: str) -> bool:
    """Check if table is likely a department table"""
    return classify_table_purpose(table_name) == 'department'

def map_semantic_columns(columns: list) -> dict:
    """Map columns to semantic meanings"""
    mapping = {}
    column_names = [col['name'].lower() for col in columns]
    
    # Define semantic patterns
    patterns = {
        'id': ['id', 'emp_id', 'employee_id', 'person_id', 'staff_id'],
        'name': ['name', 'full_name', 'employee_name', 'first_name'],
        'salary': ['salary', 'annual_salary', 'compensation', 'pay', 'pay_rate'],
        'department': ['department', 'dept', 'division', 'dept_id', 'dept_name'],
        'position': ['position', 'title', 'role', 'job_title'],
        'hire_date': ['join_date', 'hire_date', 'hired_on', 'start_date']
    }
    
    for semantic_name, pattern_list in patterns.items():
        for col_name in column_names:
            for pattern in pattern_list:
                if pattern in col_name:
                    original_name = next(col['name'] for col in columns if col['name'].lower() == col_name)
                    mapping[semantic_name] = original_name
                    break
            if semantic_name in mapping:
                break
    
    return mapping

@schema_bp.route("/disconnect", methods=["POST"])
def disconnect_database():
    """Disconnect from database and cleanup"""
    global _global_db_state, current_schema, current_schema_info
    
    try:
        if _global_db_state['engine']:
            _global_db_state['engine'].dispose()
            logger.info("Database connection pool disposed")
        
        _global_db_state = {
            'connected': False,
            'schema': None,
            'engine': None,
            'connection_string': None,
            'connection_pool_size': 10,
            'max_overflow': 20,
            'pool_recycle': 3600
        }
        
        current_schema = None
        current_schema_info = None
        
        return jsonify({
            "ok": True,
            "message": "Database disconnected successfully"
        })
        
    except Exception as e:
        logger.error(f"Error disconnecting: {e}")
        return jsonify({
            "ok": False,
            "error": "disconnect_error",
            "message": str(e)
        }), 500

@schema_bp.route("/schema", methods=["GET"])
def get_schema():
    """Get current schema information"""
    if not _global_db_state['connected'] or not current_schema_info:
        return jsonify({
            "ok": False,
            "error": "not_connected",
            "message": "No database connected"
        }), 400
    
    return jsonify({
        "ok": True,
        "schema": current_schema_info
    })

@schema_bp.route("/schema-debug", methods=["GET"])
def schema_debug():
    """Debug endpoint for schema blueprint"""
    return jsonify({
        "ok": True,
        "message": "Schema blueprint is working!",
        "endpoints": [
            "/test-connection (POST)",
            "/connect-database (POST)",
            "/disconnect (POST)",
            "/schema (GET)",
            "/schema-debug (GET)"
        ],
        "current_schema": current_schema is not None,
        "global_db_connected": _global_db_state['connected'],
        "tables": list(_global_db_state['schema'].keys()) if _global_db_state['schema'] else [],
        "pool_info": {
            "size": _global_db_state['connection_pool_size'],
            "overflow": _global_db_state['max_overflow']
        } if _global_db_state['connected'] else None
    })