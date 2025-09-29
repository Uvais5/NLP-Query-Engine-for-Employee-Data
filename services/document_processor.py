# import PyPDF2
# from sentence_transformers import SentenceTransformer
# import faiss
# import tempfile

# model = SentenceTransformer("all-MiniLM-L6-v2")

# def process_document(file):
#     text = ""
#     # Save file temporarily
#     with tempfile.NamedTemporaryFile(delete=False) as tmp:
#         file.save(tmp.name)
#         if file.filename.endswith(".pdf"):
#             reader = PyPDF2.PdfReader(tmp.name)
#             for page in reader.pages:
#                 text += page.extract_text() or ""
#         else:
#             text = file.read().decode("utf-8")
    
#     # Split text into chunks
#     chunks = text.split("\n\n")
#     embeddings = model.encode(chunks)
#     index = faiss.IndexFlatL2(embeddings.shape[1])
#     index.add(embeddings)
#     return index, chunks
import PyPDF2
import docx
from sentence_transformers import SentenceTransformer
import faiss
import tempfile
import os
import logging
from typing import Dict, List, Tuple
import numpy as np
import re
from datetime import datetime

logger = logging.getLogger(__name__)

class DocumentProcessor:
    """Production-ready document processor with batch embedding and dynamic chunking"""
    
    def __init__(self, model_name="all-MiniLM-L6-v2", batch_size=32):
        self.model = SentenceTransformer(model_name)
        self.batch_size = batch_size
        logger.info(f"Initialized DocumentProcessor with model: {model_name}, batch_size: {batch_size}")
    
    def process_document(self, file) -> Dict:
        """Process a single document with dynamic chunking"""
        try:
            filename = file.filename
            file_ext = self._get_file_extension(filename)
            
            logger.info(f"Processing document: {filename} (type: {file_ext})")
            
            # Extract text based on file type
            text = self._extract_text(file, file_ext)
            
            if not text or len(text.strip()) < 10:
                raise ValueError(f"No meaningful text extracted from {filename}")
            
            # Dynamic chunking based on document type and structure
            chunks = self._dynamic_chunk(text, file_ext, filename)
            
            if not chunks:
                raise ValueError(f"No chunks created from {filename}")
            
            logger.info(f"Created {len(chunks)} chunks from {filename}")
            
            # Batch process embeddings for efficiency
            embeddings = self._batch_encode(chunks)
            
            # Create FAISS index
            index = self._create_faiss_index(embeddings)
            
            # Extract metadata
            metadata = self._extract_metadata(text, file_ext, filename)
            
            return {
                'index': index,
                'chunks': chunks,
                'embeddings': embeddings,
                'metadata': metadata
            }
            
        except Exception as e:
            logger.error(f"Error processing document {file.filename}: {e}", exc_info=True)
            raise
    
    def _get_file_extension(self, filename: str) -> str:
        """Get normalized file extension"""
        if '.' not in filename:
            return 'txt'
        return filename.rsplit('.', 1)[1].lower()
    
    def _extract_text(self, file, file_ext: str) -> str:
        """Extract text from various file formats"""
        text = ""
        
        try:
            # Save to temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{file_ext}') as tmp:
                file.save(tmp.name)
                tmp_path = tmp.name
            
            try:
                if file_ext == 'pdf':
                    text = self._extract_from_pdf(tmp_path)
                elif file_ext in ['docx', 'doc']:
                    text = self._extract_from_docx(tmp_path)
                elif file_ext in ['txt', 'md', 'csv']:
                    with open(tmp_path, 'r', encoding='utf-8', errors='ignore') as f:
                        text = f.read()
                else:
                    # Fallback: try to read as text
                    with open(tmp_path, 'r', encoding='utf-8', errors='ignore') as f:
                        text = f.read()
            finally:
                # Clean up temp file
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            
            return text
            
        except Exception as e:
            logger.error(f"Text extraction error: {e}")
            raise ValueError(f"Failed to extract text: {e}")
    
    def _extract_from_pdf(self, filepath: str) -> str:
        """Extract text from PDF"""
        text = ""
        try:
            with open(filepath, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                for page_num, page in enumerate(reader.pages):
                    page_text = page.extract_text()
                    if page_text:
                        text += f"\n--- Page {page_num + 1} ---\n"
                        text += page_text
        except Exception as e:
            logger.error(f"PDF extraction error: {e}")
            raise
        return text
    
    def _extract_from_docx(self, filepath: str) -> str:
        """Extract text from DOCX"""
        try:
            doc = docx.Document(filepath)
            paragraphs = [para.text for para in doc.paragraphs if para.text.strip()]
            return '\n\n'.join(paragraphs)
        except Exception as e:
            logger.error(f"DOCX extraction error: {e}")
            # Fallback to reading as text
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
    
    def _dynamic_chunk(self, text: str, file_ext: str, filename: str) -> List[str]:
        """
        Intelligent chunking based on document structure and type.
        Preserves semantic boundaries instead of fixed token limits.
        """
        # Determine document type
        doc_type = self._detect_document_type(text, filename)
        
        if doc_type == 'resume':
            return self._chunk_resume(text)
        elif doc_type == 'technical':
            return self._chunk_technical(text)
        elif file_ext == 'csv':
            return self._chunk_csv(text)
        else:
            return self._chunk_general(text)
    
    def _detect_document_type(self, text: str, filename: str) -> str:
        """Detect document type for optimized chunking"""
        text_lower = text.lower()
        filename_lower = filename.lower()
        
        # Resume indicators
        resume_keywords = ['experience', 'education', 'skills', 'employment', 'cv', 'resume']
        if any(keyword in filename_lower for keyword in ['resume', 'cv']) or \
           sum(keyword in text_lower for keyword in resume_keywords) >= 3:
            return 'resume'
        
        # Technical document indicators
        technical_keywords = ['function', 'class', 'import', 'def ', 'algorithm', 'implementation']
        if sum(keyword in text_lower for keyword in technical_keywords) >= 2:
            return 'technical'
        
        return 'general'
    
    def _chunk_resume(self, text: str) -> List[str]:
        """Chunk resume while preserving section integrity"""
        # Common resume section headers
        section_patterns = [
            r'\n(experience|work experience|employment history)[\s\n]*:?\s*\n',
            r'\n(education|academic background)[\s\n]*:?\s*\n',
            r'\n(skills|technical skills|core competencies)[\s\n]*:?\s*\n',
            r'\n(projects|key projects)[\s\n]*:?\s*\n',
            r'\n(certifications?|licenses?)[\s\n]*:?\s*\n',
            r'\n(summary|professional summary|objective)[\s\n]*:?\s*\n'
        ]
        
        chunks = []
        current_pos = 0
        
        # Find all section boundaries
        boundaries = []
        for pattern in section_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                boundaries.append(match.start())
        
        boundaries.sort()
        boundaries.append(len(text))
        
        # Create chunks based on sections
        for i in range(len(boundaries)):
            if i == 0:
                chunk = text[current_pos:boundaries[i]].strip()
            else:
                chunk = text[boundaries[i-1]:boundaries[i]].strip()
            
            if chunk and len(chunk) > 50:
                # If section is too large, split it further
                if len(chunk) > 1500:
                    sub_chunks = self._split_large_chunk(chunk, max_size=1000)
                    chunks.extend(sub_chunks)
                else:
                    chunks.append(chunk)
        
        return chunks if chunks else self._chunk_general(text)
    
    def _chunk_technical(self, text: str) -> List[str]:
        """Chunk technical documents preserving code blocks and explanations"""
        chunks = []
        
        # Split on double newlines (paragraphs)
        paragraphs = text.split('\n\n')
        
        current_chunk = ""
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            # If adding this paragraph exceeds reasonable size, save current chunk
            if len(current_chunk) + len(para) > 800 and current_chunk:
                chunks.append(current_chunk.strip())
                current_chunk = para
            else:
                current_chunk += "\n\n" + para if current_chunk else para
        
        # Add remaining chunk
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks if chunks else self._chunk_general(text)
    
    def _chunk_csv(self, text: str) -> List[str]:
        """Chunk CSV data by rows with header context"""
        lines = text.split('\n')
        if not lines:
            return [text]
        
        # First line is likely header
        header = lines[0]
        chunks = []
        
        # Group rows into reasonable chunks (20 rows per chunk)
        chunk_size = 20
        for i in range(1, len(lines), chunk_size):
            chunk_lines = [header] + lines[i:i+chunk_size]
            chunk = '\n'.join(chunk_lines)
            if chunk.strip():
                chunks.append(chunk)
        
        return chunks if chunks else [text]
    
    def _chunk_general(self, text: str, chunk_size: int = 800, overlap: int = 100) -> List[str]:
        """General purpose chunking with overlap"""
        # Clean text
        text = re.sub(r'\n{3,}', '\n\n', text)  # Remove excessive newlines
        text = re.sub(r' {2,}', ' ', text)  # Remove excessive spaces
        
        # Split into sentences
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            # If adding this sentence exceeds chunk_size and we have content
            if len(current_chunk) + len(sentence) > chunk_size and current_chunk:
                chunks.append(current_chunk.strip())
                # Start new chunk with overlap (last few sentences)
                overlap_text = ' '.join(current_chunk.split()[-overlap:]) if overlap > 0 else ""
                current_chunk = overlap_text + " " + sentence
            else:
                current_chunk += " " + sentence if current_chunk else sentence
        
        # Add final chunk
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks if chunks else [text]
    
    def _split_large_chunk(self, text: str, max_size: int = 1000) -> List[str]:
        """Split a large chunk into smaller pieces"""
        sentences = re.split(r'(?<=[.!?])\s+', text)
        chunks = []
        current = ""
        
        for sent in sentences:
            if len(current) + len(sent) > max_size and current:
                chunks.append(current.strip())
                current = sent
            else:
                current += " " + sent if current else sent
        
        if current.strip():
            chunks.append(current.strip())
        
        return chunks
    
    def _batch_encode(self, chunks: List[str]) -> np.ndarray:
        """Encode chunks in batches for efficiency"""
        if not chunks:
            return np.array([])
        
        all_embeddings = []
        
        # Process in batches
        for i in range(0, len(chunks), self.batch_size):
            batch = chunks[i:i + self.batch_size]
            batch_embeddings = self.model.encode(
                batch,
                batch_size=self.batch_size,
                show_progress_bar=False,
                convert_to_numpy=True
            )
            all_embeddings.append(batch_embeddings)
        
        # Concatenate all batches
        embeddings = np.vstack(all_embeddings)
        logger.info(f"Batch encoded {len(chunks)} chunks, embeddings shape: {embeddings.shape}")
        
        return embeddings
    
    def _create_faiss_index(self, embeddings: np.ndarray):
        """Create FAISS index for efficient similarity search"""
        if len(embeddings) == 0:
            raise ValueError("Cannot create index from empty embeddings")
        
        dimension = embeddings.shape[1]
        
        # Use IndexFlatL2 for small datasets, IndexIVFFlat for large datasets
        if len(embeddings) < 1000:
            index = faiss.IndexFlatL2(dimension)
        else:
            # For larger datasets, use IVF index
            nlist = min(100, len(embeddings) // 10)  # Number of clusters
            quantizer = faiss.IndexFlatL2(dimension)
            index = faiss.IndexIVFFlat(quantizer, dimension, nlist)
            index.train(embeddings)
        
        index.add(embeddings)
        logger.info(f"Created FAISS index with {index.ntotal} vectors")
        
        return index
    
    def _extract_metadata(self, text: str, file_ext: str, filename: str) -> Dict:
        """Extract metadata from document"""
        metadata = {
            'filename': filename,
            'file_type': file_ext,
            'character_count': len(text),
            'word_count': len(text.split()),
            'extracted_at': datetime.now().isoformat()
        }
        
        # Extract potential entities
        text_lower = text.lower()
        
        # Skills detection
        common_skills = [
            'python', 'java', 'javascript', 'react', 'angular', 'vue', 'node',
            'sql', 'mongodb', 'postgresql', 'aws', 'azure', 'docker', 'kubernetes',
            'machine learning', 'deep learning', 'tensorflow', 'pytorch', 'nlp',
            'computer vision', 'data science', 'analytics', 'tableau', 'power bi'
        ]
        
        detected_skills = [skill for skill in common_skills if skill in text_lower]
        if detected_skills:
            metadata['detected_skills'] = detected_skills
        
        return metadata