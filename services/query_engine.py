from sentence_transformers import SentenceTransformer
import numpy as np
import re
from typing import Dict, List, Any, Optional
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import time

class EnhancedQueryEngine:
    def __init__(self, model_name="all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)
        self.tfidf = TfidfVectorizer(stop_words='english', max_features=1000)
        self.query_cache = {}  # Simple in-memory cache
        
    def preprocess_query(self, query: str) -> Dict[str, Any]:
        """Enhanced query preprocessing with intent detection"""
        query_lower = query.lower().strip()
        
        # Intent detection
        intent = self._detect_intent(query_lower)
        
        # Extract keywords and phrases
        keywords = self._extract_keywords(query_lower)
        
        # Extract entities (skills, roles, etc.)
        entities = self._extract_entities(query_lower)
        
        return {
            "original": query,
            "processed": query_lower,
            "intent": intent,
            "keywords": keywords,
            "entities": entities
        }
    
    def _detect_intent(self, query: str) -> str:
        """Detect query intent"""
        if any(word in query for word in ['find', 'show', 'get', 'list']):
            return 'search'
        elif any(word in query for word in ['count', 'how many']):
            return 'count'
        elif any(word in query for word in ['compare', 'vs', 'versus']):
            return 'compare'
        else:
            return 'search'
    
    def _extract_keywords(self, query: str) -> List[str]:
        """Extract important keywords from query"""
        # Common patterns for skill/role extraction
        patterns = [
            r'mentioning\s+(.+?)(?:\s+and|\s+or|$)',
            r'with\s+(.+?)(?:\s+experience|\s+skills|$)',
            r'experienced\s+in\s+(.+?)(?:\s+and|\s+or|$)',
            r'skills?\s+in\s+(.+?)(?:\s+and|\s+or|$)'
        ]
        
        keywords = []
        for pattern in patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            for match in matches:
                # Split on common delimiters
                terms = re.split(r'[,\s+and\s+or\s+]', match)
                keywords.extend([term.strip() for term in terms if term.strip()])
        
        # Fallback: extract all meaningful words
        if not keywords:
            stop_words = {'the', 'and', 'or', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
            keywords = [word for word in query.split() if word not in stop_words and len(word) > 2]
        
        return list(set(keywords))  # Remove duplicates
    
    def _extract_entities(self, query: str) -> Dict[str, List[str]]:
        """Extract specific entities (skills, roles, departments)"""
        entities = {"skills": [], "roles": [], "departments": []}
        
        # Common tech skills
        tech_skills = [
            'python', 'java', 'javascript', 'react', 'node', 'sql', 'mongodb',
            'machine learning', 'ai', 'computer vision', 'nlp', 'deep learning',
            'tensorflow', 'pytorch', 'pandas', 'numpy', 'scikit-learn',
            'aws', 'azure', 'docker', 'kubernetes', 'git'
        ]
        
        # Common roles
        roles = [
            'developer', 'engineer', 'analyst', 'manager', 'lead', 'senior',
            'data scientist', 'software engineer', 'ml engineer', 'devops'
        ]
        
        # Check for skills and roles in query
        for skill in tech_skills:
            if skill.lower() in query:
                entities["skills"].append(skill)
        
        for role in roles:
            if role.lower() in query:
                entities["roles"].append(role)
        
        return entities
    
    def _calculate_semantic_similarity(self, query_embedding: np.ndarray, chunk_text: str) -> float:
        """Calculate semantic similarity between query and chunk"""
        chunk_embedding = self.model.encode([chunk_text])[0]
        similarity = np.dot(query_embedding, chunk_embedding) / (
            np.linalg.norm(query_embedding) * np.linalg.norm(chunk_embedding)
        )
        return float(similarity)
    
    def _calculate_keyword_score(self, keywords: List[str], entities: Dict[str, List[str]], chunk_text: str) -> float:
        """Enhanced keyword scoring with entity boosting"""
        chunk_lower = chunk_text.lower()
        score = 0.0
        
        # Exact keyword matches
        for keyword in keywords:
            if keyword.lower() in chunk_lower:
                score += 1.0
        
        # Entity boosting (skills are more important)
        for skill in entities.get("skills", []):
            if skill.lower() in chunk_lower:
                score += 2.0  # Higher boost for skills
        
        for role in entities.get("roles", []):
            if role.lower() in chunk_lower:
                score += 1.5  # Medium boost for roles
        
        # Phrase matching bonus
        full_keywords = " ".join(keywords).lower()
        if len(keywords) > 1 and full_keywords in chunk_lower:
            score += 2.0
        
        return score
    
    def _calculate_document_relevance(self, query_info: Dict[str, Any], doc_chunks: List[str]) -> float:
        """Calculate overall document relevance"""
        relevance_scores = []
        
        for chunk in doc_chunks[:5]:  # Check top 5 chunks
            keyword_score = self._calculate_keyword_score(
                query_info["keywords"], 
                query_info["entities"], 
                chunk
            )
            relevance_scores.append(keyword_score)
        
        return max(relevance_scores) if relevance_scores else 0.0
    
    def _apply_confidence_threshold(self, results: List[Dict], threshold: float = 0.3) -> List[Dict]:
        """Filter out low-confidence results"""
        return [r for r in results if r.get("confidence", 0) >= threshold]
    
    def handle_query(self, user_query: str, schema: Optional[Dict], documents_index: Optional[Dict], 
                    top_k: int = 5, confidence_threshold: float = 0.3) -> Dict[str, Any]:
        """
        Enhanced hybrid query processing with better ranking and filtering
        """
        start_time = time.time()
        
        # Check cache first
        cache_key = f"{user_query}_{top_k}"
        if cache_key in self.query_cache:
            cached_result = self.query_cache[cache_key].copy()
            cached_result["cached"] = True
            cached_result["processing_time"] = time.time() - start_time
            return cached_result
        
        # Preprocess query
        query_info = self.preprocess_query(user_query)
        query_embedding = self.model.encode([query_info["processed"]])[0]
        
        # 1. SQL/Schema part
        sql_result = None
        if schema:
            sql_result = {
                "tables": list(schema.keys()), 
                "columns": schema,
                "suggested_queries": self._generate_sql_suggestions(query_info, schema)
            }
        
        # 2. Enhanced document search
        doc_results = []
        
        if documents_index:
            for fname, doc in documents_index.items():
                index, chunks = doc["index"], doc["chunks"]
                
                # Get semantic similarity scores
                scores, ids = index.search(np.array([query_embedding]), k=min(top_k * 2, len(chunks)))
                
                # Calculate document-level relevance
                doc_relevance = self._calculate_document_relevance(query_info, chunks)
                
                # Process each matching chunk
                chunk_results = []
                for score, idx in zip(scores[0], ids[0]):
                    if idx != -1 and idx < len(chunks):
                        chunk_text = chunks[idx]
                        
                        # Enhanced scoring
                        semantic_score = float(1.0 - score)  # Convert distance to similarity
                        keyword_score = self._calculate_keyword_score(
                            query_info["keywords"], 
                            query_info["entities"], 
                            chunk_text
                        )
                        
                        # Weighted final score
                        final_score = (
                            semantic_score * 0.4 +  # Semantic similarity
                            keyword_score * 0.4 +   # Keyword matching
                            doc_relevance * 0.2     # Document relevance
                        )
                        
                        # Calculate confidence
                        confidence = min(final_score / 3.0, 1.0)  # Normalize to 0-1
                        
                        chunk_results.append({
                            "filename": fname,
                            "chunk": chunk_text[:500] + "..." if len(chunk_text) > 500 else chunk_text,
                            "score": final_score,
                            "confidence": confidence,
                            "semantic_score": semantic_score,
                            "keyword_score": keyword_score,
                            "matched_keywords": [k for k in query_info["keywords"] if k.lower() in chunk_text.lower()],
                            "matched_entities": {
                                k: [e for e in v if e.lower() in chunk_text.lower()] 
                                for k, v in query_info["entities"].items()
                            }
                        })
                
                # Add best chunks for this document
                if chunk_results:
                    # Sort by score and take top chunks per document
                    chunk_results.sort(key=lambda x: x["score"], reverse=True)
                    doc_results.extend(chunk_results[:2])  # Top 2 chunks per document
        
        # Sort all results by score
        doc_results.sort(key=lambda x: x["score"], reverse=True)
        
        # Apply confidence filtering
        doc_results = self._apply_confidence_threshold(doc_results, confidence_threshold)
        
        # Limit to top_k results
        doc_results = doc_results[:top_k]
        
        # Prepare final result
        result = {
            "db": sql_result,
            "documents": doc_results,
            "query_info": query_info,
            "processing_time": time.time() - start_time,
            "total_results": len(doc_results),
            "cached": False
        }
        
        # Cache the result
        self.query_cache[cache_key] = result.copy()
        
        return result
    
    def _generate_sql_suggestions(self, query_info: Dict[str, Any], schema: Dict) -> List[str]:
        """Generate SQL query suggestions based on the natural language query"""
        suggestions = []
        
        # Simple heuristics for common queries
        if query_info["intent"] == "count":
            for table in schema.keys():
                suggestions.append(f"SELECT COUNT(*) FROM {table}")
        
        elif query_info["intent"] == "search":
            for table, columns in schema.items():
                # Look for text/varchar columns that might contain the search terms
                text_columns = [col for col in columns if any(
                    keyword in str(col).lower() for keyword in ['name', 'description', 'text', 'varchar']
                )]
                if text_columns:
                    for keyword in query_info["keywords"][:2]:  # Limit to first 2 keywords
                        col = text_columns[0]
                        suggestions.append(f"SELECT * FROM {table} WHERE {col} LIKE '%{keyword}%'")
        
        return suggestions[:3]  # Return top 3 suggestions

# Global instance
query_engine = EnhancedQueryEngine()

# Wrapper function for backward compatibility
def handle_query(user_query, schema, documents_index, top_k=5, boost_keywords=True):
    """
    Backward compatible wrapper for the enhanced query engine
    """
    return query_engine.handle_query(
        user_query=user_query,
        schema=schema,
        documents_index=documents_index,
        top_k=top_k,
        confidence_threshold=0.3
    )