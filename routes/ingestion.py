# from flask import Blueprint, request, jsonify
# from services.document_processor import process_document

# ingestion_bp = Blueprint("ingestion", __name__)
# documents_index = {}

# @ingestion_bp.route("/upload-documents", methods=["POST"])
# def upload_documents():
#     global documents_index
#     # check if files are in request
#     if "file" not in request.files:
#         return jsonify({"status": "error", "message": "No file uploaded"})
    
#     files = request.files.getlist("file")  # <-- handle multiple files
#     processed_files = []

#     for file in files:
#         try:
#             index, chunks = process_document(file)
#             documents_index[file.filename] = {"index": index, "chunks": chunks}
#             processed_files.append(file.filename)
#         except Exception as e:
#             return jsonify({"status": "error", "message": f"{file.filename} failed: {str(e)}"})

#     return jsonify({"status": "success", "message": f"Processed files: {', '.join(processed_files)}"})
from flask import Blueprint, request, jsonify
import logging
import uuid
from datetime import datetime
from typing import Dict, List
import threading

logger = logging.getLogger(__name__)

# Import the enhanced DocumentProcessor
try:
    from services.document_processor import DocumentProcessor
    document_processor = DocumentProcessor(model_name="all-MiniLM-L6-v2", batch_size=32)
    PROCESSOR_OK = True
except ImportError as e:
    logger.error(f"Failed to import DocumentProcessor: {e}")
    PROCESSOR_OK = False

ingestion_bp = Blueprint("ingestion", __name__)

# Global storage
documents_index = {}
ingestion_jobs = {}  # Track background processing jobs
ingestion_stats = {
    'total_documents': 0,
    'total_chunks': 0,
    'total_files_processed': 0,
    'total_failures': 0,
    'last_upload': None
}

class IngestionJob:
    """Track document ingestion progress"""
    def __init__(self, job_id: str, file_count: int):
        self.job_id = job_id
        self.file_count = file_count
        self.processed = 0
        self.failed = 0
        self.status = 'processing'
        self.start_time = datetime.now()
        self.end_time = None
        self.errors = []
        self.processed_files = []
        
    def update_progress(self, filename: str, success: bool, error: str = None):
        """Update job progress"""
        if success:
            self.processed += 1
            self.processed_files.append(filename)
        else:
            self.failed += 1
            self.errors.append({'filename': filename, 'error': error})
        
        # Check if job is complete
        if self.processed + self.failed >= self.file_count:
            self.status = 'completed' if self.failed == 0 else 'completed_with_errors'
            self.end_time = datetime.now()
    
    def to_dict(self):
        """Convert to dictionary for JSON response"""
        duration = None
        if self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        elif self.status == 'processing':
            duration = (datetime.now() - self.start_time).total_seconds()
            
        return {
            'job_id': self.job_id,
            'status': self.status,
            'total_files': self.file_count,
            'processed': self.processed,
            'failed': self.failed,
            'progress_percent': round((self.processed + self.failed) / self.file_count * 100, 1) if self.file_count > 0 else 0,
            'duration_seconds': round(duration, 2) if duration else None,
            'errors': self.errors,
            'processed_files': self.processed_files
        }

def process_file_async(file, job: IngestionJob):
    """Process a single file asynchronously"""
    try:
        if not PROCESSOR_OK:
            raise Exception("DocumentProcessor not available")
        
        # Process document using enhanced processor
        result = document_processor.process_document(file)
        
        # Store in global index
        documents_index[file.filename] = {
            'index': result['index'],
            'chunks': result['chunks'],
            'embeddings': result['embeddings'],
            'metadata': result['metadata'],
            'uploaded_at': datetime.now().isoformat()
        }
        
        # Update stats
        ingestion_stats['total_documents'] += 1
        ingestion_stats['total_chunks'] += len(result['chunks'])
        ingestion_stats['total_files_processed'] += 1
        ingestion_stats['last_upload'] = datetime.now().isoformat()
        
        # Update job progress
        job.update_progress(file.filename, True)
        
        logger.info(f"Successfully processed {file.filename}: {len(result['chunks'])} chunks")
        
    except Exception as e:
        logger.error(f"Error processing {file.filename}: {e}", exc_info=True)
        job.update_progress(file.filename, False, str(e))
        ingestion_stats['total_failures'] += 1

@ingestion_bp.route("/upload-documents", methods=["POST"])
def upload_documents():
    """
    Upload and process multiple documents with progress tracking.
    Returns a job_id for tracking processing status.
    """
    global documents_index, ingestion_jobs
    
    if not PROCESSOR_OK:
        return jsonify({
            "ok": False,
            "error": "processor_unavailable",
            "message": "Document processor not available"
        }), 500
    
    # Validate request
    if "file" not in request.files:
        return jsonify({
            "ok": False,
            "error": "no_files",
            "message": "No files uploaded"
        }), 400
    
    files = request.files.getlist("file")
    
    if not files:
        return jsonify({
            "ok": False,
            "error": "empty_files",
            "message": "No files provided"
        }), 400
    
    # Validate file sizes (50MB limit per file)
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
    for file in files:
        file.seek(0, 2)  # Seek to end
        size = file.tell()
        file.seek(0)  # Reset to beginning
        
        if size > MAX_FILE_SIZE:
            return jsonify({
                "ok": False,
                "error": "file_too_large",
                "message": f"File {file.filename} exceeds 50MB limit"
            }), 413
    
    # Create job for tracking
    job_id = str(uuid.uuid4())
    job = IngestionJob(job_id, len(files))
    ingestion_jobs[job_id] = job
    
    logger.info(f"Starting ingestion job {job_id} for {len(files)} files")
    
    # Process files in background threads
    threads = []
    for file in files:
        # Create a copy of file data since Flask's file objects aren't thread-safe
        from werkzeug.datastructures import FileStorage
        from io import BytesIO
        
        file_data = BytesIO(file.read())
        file_copy = FileStorage(
            stream=file_data,
            filename=file.filename,
            content_type=file.content_type
        )
        
        thread = threading.Thread(target=process_file_async, args=(file_copy, job))
        thread.start()
        threads.append(thread)
    
    # Don't wait for threads to complete - return immediately with job_id
    return jsonify({
        "ok": True,
        "job_id": job_id,
        "message": f"Processing {len(files)} files",
        "file_count": len(files)
    }), 202  # 202 Accepted

@ingestion_bp.route("/ingest/status/<job_id>", methods=["GET"])
@ingestion_bp.route("/ingestion-status/<job_id>", methods=["GET"])
def get_ingestion_status(job_id: str):
    """
    Get the status of a document ingestion job.
    Required endpoint: GET /api/ingest/status
    """
    if job_id not in ingestion_jobs:
        return jsonify({
            "ok": False,
            "error": "job_not_found",
            "message": f"Job {job_id} not found"
        }), 404
    
    job = ingestion_jobs[job_id]
    
    return jsonify({
        "ok": True,
        "job": job.to_dict()
    })

@ingestion_bp.route("/ingest/stats", methods=["GET"])
def get_stats():
    """Get overall ingestion statistics"""
    return jsonify({
        "ok": True,
        "stats": {
            **ingestion_stats,
            'active_jobs': len([j for j in ingestion_jobs.values() if j.status == 'processing']),
            'total_jobs': len(ingestion_jobs)
        }
    })

def get_ingestion_stats():
    """Helper function for app.py metrics endpoint"""
    return {
        **ingestion_stats,
        'active_jobs': len([j for j in ingestion_jobs.values() if j.status == 'processing']),
        'total_jobs': len(ingestion_jobs),
        'documents_in_index': len(documents_index)
    }

@ingestion_bp.route("/documents", methods=["GET"])
def list_documents():
    """List all uploaded documents"""
    docs = []
    for filename, doc_data in documents_index.items():
        docs.append({
            'filename': filename,
            'chunk_count': len(doc_data['chunks']),
            'uploaded_at': doc_data.get('uploaded_at'),
            'metadata': doc_data.get('metadata', {})
        })
    
    return jsonify({
        "ok": True,
        "documents": docs,
        "total": len(docs)
    })

@ingestion_bp.route("/documents/<filename>", methods=["DELETE"])
def delete_document(filename: str):
    """Delete a document from the index"""
    if filename not in documents_index:
        return jsonify({
            "ok": False,
            "error": "not_found",
            "message": f"Document {filename} not found"
        }), 404
    
    del documents_index[filename]
    ingestion_stats['total_documents'] -= 1
    
    return jsonify({
        "ok": True,
        "message": f"Document {filename} deleted"
    })