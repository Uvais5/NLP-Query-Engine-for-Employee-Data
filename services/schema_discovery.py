from sqlalchemy import create_engine, inspect, text, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import Dict, List, Any, Optional
import re

logger = logging.getLogger(__name__)

class SQLAlchemySchemaDiscovery:
    """Enhanced schema discovery using SQLAlchemy for better database abstraction"""
    
    def __init__(self):
        self.engine = None
        self.session = None
        self.inspector = None
        self.metadata = None
        self.db_type = None
        self.schema_info = {}
        
        # Enhanced semantic mappings
        self.semantic_mappings = {
            'employee_tables': [
                'employees', 'employee', 'emp', 'staff', 'personnel', 
                'workers', 'team_members', 'people', 'users'
            ],
            'department_tables': [
                'departments', 'department', 'dept', 'divisions', 
                'division', 'teams', 'units', 'groups'
            ],
            'document_tables': [
                'documents', 'document', 'files', 'attachments', 'docs'
            ],
            'name_columns': [
                'name', 'full_name', 'employee_name', 'first_name', 
                'last_name', 'display_name', 'username', 'fname', 'lname'
            ],
            'salary_columns': [
                'salary', 'annual_salary', 'compensation', 'pay', 
                'pay_rate', 'wage', 'income', 'earnings', 'remuneration'
            ],
            'department_columns': [
                'department', 'dept', 'division', 'dept_name', 
                'department_name', 'division_name', 'team', 'unit'
            ],
            'position_columns': [
                'position', 'title', 'role', 'job_title', 'designation', 
                'rank', 'level', 'job_role'
            ],
            'date_columns': [
                'join_date', 'hire_date', 'hired_on', 'start_date', 
                'employment_date', 'created_at', 'date_joined'
            ],
            'id_columns': [
                'id', 'emp_id', 'employee_id', 'person_id', 'staff_id', 
                'user_id', 'pk', 'primary_key'
            ]
        }
    
    def normalize_connection_string(self, connection_string: str) -> str:
        """Convert various connection string formats to SQLAlchemy format"""
        conn_str = connection_string.strip()
        
        # XAMPP MySQL shortcuts
        if conn_str.startswith('localhost/') or (not '://' in conn_str and '/' in conn_str):
            # Format: localhost/database or localhost:3306/database
            if ':' in conn_str.split('/')[0]:
                host_port, database = conn_str.split('/', 1)
                host, port = host_port.split(':')
            else:
                host, database = conn_str.split('/', 1)
                port = '3306'
            
            return f"mysql+pymysql://root:@{host}:{port}/{database}"
        
        # Handle mysql:// format (convert to mysql+pymysql://)
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
    
    def connect_to_database(self, connection_string: str) -> bool:
        """Connect to database using SQLAlchemy"""
        try:
            # Normalize connection string
            normalized_conn_str = self.normalize_connection_string(connection_string)
            logger.info(f"Connecting with: {normalized_conn_str}")
            
            # Determine database type
            if 'mysql' in normalized_conn_str:
                self.db_type = 'mysql'
            elif 'postgresql' in normalized_conn_str or 'postgres' in normalized_conn_str:
                self.db_type = 'postgresql'
            elif 'sqlite' in normalized_conn_str:
                self.db_type = 'sqlite'
            else:
                self.db_type = 'unknown'
            
            # Create engine
            self.engine = create_engine(normalized_conn_str, echo=False)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            # Create inspector for schema discovery
            self.inspector = inspect(self.engine)
            
            # Create metadata object
            self.metadata = MetaData()
            self.metadata.reflect(bind=self.engine)
            
            # Create session
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            
            logger.info(f"Successfully connected to {self.db_type} database")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error: {e}")
            raise ConnectionError(f"Database connection failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error connecting to database: {e}")
            raise ConnectionError(f"Could not connect to database: {e}")
    
    def discover_tables_and_columns(self) -> Dict[str, Any]:
        """Discover all tables and their columns using SQLAlchemy inspector"""
        try:
            tables_info = {}
            table_names = self.inspector.get_table_names()
            
            logger.info(f"Found {len(table_names)} tables: {table_names}")
            
            for table_name in table_names:
                # Get columns
                columns = self.inspector.get_columns(table_name)
                
                # Get primary keys
                pk_constraint = self.inspector.get_pk_constraint(table_name)
                primary_keys = pk_constraint.get('constrained_columns', [])
                
                # Get foreign keys
                foreign_keys = self.inspector.get_foreign_keys(table_name)
                
                # Get indexes
                indexes = self.inspector.get_indexes(table_name)
                
                # Process columns with enhanced info
                processed_columns = []
                for col in columns:
                    col_info = {
                        'name': col['name'],
                        'type': str(col['type']),
                        'nullable': col['nullable'],
                        'default': col.get('default'),
                        'primary_key': col['name'] in primary_keys,
                        'foreign_key': any(col['name'] in fk['constrained_columns'] for fk in foreign_keys),
                        'indexed': any(col['name'] in idx['column_names'] for idx in indexes)
                    }
                    processed_columns.append(col_info)
                
                # Classify table purpose
                purpose = self.classify_table_purpose(table_name, processed_columns)
                
                # Map semantic columns
                semantic_mapping = self.map_semantic_columns(processed_columns)
                
                # Get sample data
                sample_data = self.get_sample_data(table_name, 3)
                
                tables_info[table_name] = {
                    'columns': processed_columns,
                    'foreign_keys': foreign_keys,
                    'indexes': indexes,
                    'purpose': purpose,
                    'semantic_mapping': semantic_mapping,
                    'sample_data': sample_data,
                    'row_count': self.get_table_row_count(table_name)
                }
            
            return tables_info
            
        except Exception as e:
            logger.error(f"Error discovering tables and columns: {e}")
            raise e
    
    def discover_relationships(self) -> List[Dict[str, Any]]:
        """Discover relationships between tables"""
        try:
            relationships = []
            table_names = self.inspector.get_table_names()
            
            for table_name in table_names:
                foreign_keys = self.inspector.get_foreign_keys(table_name)
                
                for fk in foreign_keys:
                    relationship = {
                        'from_table': table_name,
                        'from_columns': fk['constrained_columns'],
                        'to_table': fk['referred_table'],
                        'to_columns': fk['referred_columns'],
                        'constraint_name': fk.get('name', ''),
                        'type': 'foreign_key'
                    }
                    relationships.append(relationship)
            
            # Try to infer additional relationships based on naming patterns
            implicit_relationships = self.infer_implicit_relationships(table_names)
            relationships.extend(implicit_relationships)
            
            return relationships
            
        except Exception as e:
            logger.error(f"Error discovering relationships: {e}")
            return []
    
    def infer_implicit_relationships(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Infer relationships based on common naming patterns"""
        implicit_rels = []
        
        for table in table_names:
            columns = self.inspector.get_columns(table)
            
            for col in columns:
                col_name = col['name'].lower()
                
                # Look for foreign key patterns like dept_id, department_id, etc.
                for other_table in table_names:
                    if other_table == table:
                        continue
                    
                    other_table_lower = other_table.lower()
                    
                    # Check patterns like dept_id -> departments, division -> divisions
                    patterns = [
                        f"{other_table_lower}_id",
                        f"{other_table_lower[:-1]}_id" if other_table_lower.endswith('s') else f"{other_table_lower}s_id",
                        other_table_lower[:4] + "_id" if len(other_table_lower) > 4 else None
                    ]
                    
                    for pattern in patterns:
                        if pattern and pattern == col_name:
                            implicit_rels.append({
                                'from_table': table,
                                'from_columns': [col['name']],
                                'to_table': other_table,
                                'to_columns': ['id'],  # Assume 'id' is primary key
                                'type': 'inferred_foreign_key',
                                'confidence': 0.8
                            })
        
        return implicit_rels
    
    def classify_table_purpose(self, table_name: str, columns: List[Dict]) -> str:
        """Enhanced table purpose classification"""
        table_lower = table_name.lower()
        column_names = [col['name'].lower() for col in columns]
        
        # Direct name matching
        for purpose, patterns in self.semantic_mappings.items():
            if purpose.endswith('_tables'):
                purpose_name = purpose.replace('_tables', '')
                if any(pattern in table_lower for pattern in patterns):
                    return purpose_name
        
        # Column-based inference
        employee_score = 0
        department_score = 0
        document_score = 0
        
        # Score based on column presence
        for col_name in column_names:
            if any(pattern in col_name for pattern in self.semantic_mappings['name_columns'] + 
                   self.semantic_mappings['salary_columns'] + self.semantic_mappings['position_columns']):
                employee_score += 1
            
            if any(pattern in col_name for pattern in self.semantic_mappings['department_columns']):
                department_score += 1
            
            if any(pattern in col_name for pattern in ['content', 'file', 'document', 'attachment']):
                document_score += 1
        
        # Return highest scoring purpose
        if employee_score >= department_score and employee_score >= document_score:
            return 'employee'
        elif department_score > document_score:
            return 'department'
        elif document_score > 0:
            return 'document'
        else:
            return 'other'
    
    def map_semantic_columns(self, columns: List[Dict]) -> Dict[str, str]:
        """Map actual column names to semantic meanings"""
        mapping = {}
        column_names = [col['name'].lower() for col in columns]
        
        for semantic_type, patterns in self.semantic_mappings.items():
            if semantic_type.endswith('_columns'):
                semantic_name = semantic_type.replace('_columns', '')
                
                # Find best matching column
                best_match = None
                for col_name in column_names:
                    for pattern in patterns:
                        if pattern in col_name:
                            if not best_match or len(pattern) > len(best_match[1]):
                                # Find original case column name
                                original_name = next(col['name'] for col in columns if col['name'].lower() == col_name)
                                best_match = (original_name, pattern)
                
                if best_match:
                    mapping[semantic_name] = best_match[0]
        
        return mapping
    
    def get_sample_data(self, table_name: str, limit: int = 3) -> List[Dict]:
        """Get sample data using SQLAlchemy"""
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            
            with self.engine.connect() as conn:
                result = conn.execute(table.select().limit(limit))
                
                sample_data = []
                for row in result:
                    # Convert row to dict, handling various data types
                    row_dict = {}
                    for key, value in row._mapping.items():
                        # Convert non-serializable types to strings
                        if isinstance(value, (str, int, float, bool, type(None))):
                            row_dict[key] = value
                        else:
                            row_dict[key] = str(value)
                    sample_data.append(row_dict)
                
                return sample_data
            
        except Exception as e:
            logger.error(f"Error getting sample data from {table_name}: {e}")
            return []
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get total row count for a table"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.scalar()
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {e}")
            return 0
    
    def analyze_database(self, connection_string: str) -> Dict[str, Any]:
        """Main method to analyze entire database structure"""
        try:
            # Connect to database
            if not self.connect_to_database(connection_string):
                raise ConnectionError("Failed to connect to database")
            
            # Discover tables and columns
            tables_info = self.discover_tables_and_columns()
            
            # Discover relationships
            relationships = self.discover_relationships()
            
            # Create comprehensive analysis
            schema_analysis = {
                'connection_string': connection_string,
                'database_type': self.db_type,
                'tables': tables_info,
                'relationships': relationships,
                'summary': {
                    'total_tables': len(tables_info),
                    'total_columns': sum(len(table['columns']) for table in tables_info.values()),
                    'total_relationships': len(relationships),
                    'employee_tables': [name for name, info in tables_info.items() if info['purpose'] == 'employee'],
                    'department_tables': [name for name, info in tables_info.items() if info['purpose'] == 'department'],
                    'document_tables': [name for name, info in tables_info.items() if info['purpose'] == 'document'],
                    'other_tables': [name for name, info in tables_info.items() if info['purpose'] == 'other'],
                    'total_rows': sum(table.get('row_count', 0) for table in tables_info.values())
                }
            }
            
            # Store for later use
            self.schema_info = schema_analysis
            
            logger.info(f"Schema analysis complete: {schema_analysis['summary']}")
            return schema_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing database: {e}")
            raise e
    
    def generate_sql_from_natural_language(self, query: str) -> Dict[str, Any]:
        """Generate SQL based on natural language query and discovered schema"""
        query_lower = query.lower()
        
        # Determine query type
        query_type = 'select'
        if any(word in query_lower for word in ['count', 'how many', 'number of']):
            query_type = 'count'
        elif any(word in query_lower for word in ['average', 'avg', 'mean']):
            query_type = 'aggregate'
        elif any(word in query_lower for word in ['maximum', 'max', 'highest']):
            query_type = 'max'
        elif any(word in query_lower for word in ['minimum', 'min', 'lowest']):
            query_type = 'min'
        
        # Find relevant tables
        relevant_tables = []
        for table_name, table_info in self.schema_info.get('tables', {}).items():
            if table_info['purpose'] == 'employee' and any(word in query_lower for word in 
                ['employee', 'staff', 'people', 'person', 'worker']):
                relevant_tables.append(table_name)
            elif table_info['purpose'] == 'department' and any(word in query_lower for word in 
                ['department', 'division', 'team']):
                relevant_tables.append(table_name)
        
        if not relevant_tables:
            # Default to first employee table if found
            employee_tables = self.schema_info['summary'].get('employee_tables', [])
            if employee_tables:
                relevant_tables = [employee_tables[0]]
        
        # Generate SQL based on query type and relevant tables
        sql_suggestions = []
        
        if relevant_tables:
            main_table = relevant_tables[0]
            table_info = self.schema_info['tables'][main_table]
            semantic_mapping = table_info['semantic_mapping']
            
            if query_type == 'count':
                sql_suggestions.append(f"SELECT COUNT(*) FROM {main_table}")
                
                # Count by department if department column exists
                if 'department' in semantic_mapping:
                    dept_col = semantic_mapping['department']
                    sql_suggestions.append(f"SELECT {dept_col}, COUNT(*) FROM {main_table} GROUP BY {dept_col}")
            
            elif query_type == 'aggregate' and 'salary' in semantic_mapping:
                salary_col = semantic_mapping['salary']
                sql_suggestions.append(f"SELECT AVG({salary_col}) FROM {main_table}")
                
                if 'department' in semantic_mapping:
                    dept_col = semantic_mapping['department']
                    sql_suggestions.append(f"SELECT {dept_col}, AVG({salary_col}) FROM {main_table} GROUP BY {dept_col}")
            
            elif query_type == 'select':
                # Basic select with relevant columns
                select_columns = []
                if 'name' in semantic_mapping:
                    select_columns.append(semantic_mapping['name'])
                if 'department' in semantic_mapping:
                    select_columns.append(semantic_mapping['department'])
                if 'position' in semantic_mapping:
                    select_columns.append(semantic_mapping['position'])
                
                if select_columns:
                    sql_suggestions.append(f"SELECT {', '.join(select_columns)} FROM {main_table}")
        
        return {
            'query_type': query_type,
            'relevant_tables': relevant_tables,
            'sql_suggestions': sql_suggestions,
            'confidence': 0.8 if sql_suggestions else 0.2
        }
    
    def close_connection(self):
        """Close database connections"""
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()
        logger.info("Database connections closed")


# Global instance
schema_discovery = SQLAlchemySchemaDiscovery()