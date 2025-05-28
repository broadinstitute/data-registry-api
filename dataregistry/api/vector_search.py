"""
Vector search functionality using ChromaDB for semantic phenotype search.
"""

import os
import logging
from typing import List, Dict, Any, Optional
import chromadb

logger = logging.getLogger(__name__)

class PhenotypeVectorSearch:
    def __init__(self, db_path: str = "./chroma_db"):
        self.db_path = db_path
        self.client = None
        self.collection = None
        # Medical synonyms for better matching
        self.synonyms = {
            'diabetes': ['diabetes mellitus', 'T2D', 'type 2 diabetes', 'diabetes type 2'],
            'T2D': ['type 2 diabetes', 'diabetes mellitus type 2', 'diabetes'],
            'hypertension': ['high blood pressure', 'HTN'],
            'obesity': ['BMI', 'body mass index', 'overweight'],
            'CAD': ['coronary artery disease', 'heart disease', 'cardiovascular disease'],
            'stroke': ['cerebrovascular accident', 'CVA'],
            'MI': ['myocardial infarction', 'heart attack'],
        }
        self._initialize()
    
    def _initialize(self):
        """Initialize ChromaDB client and collection."""
        try:
            # Use the same embedding function as used during database creation
            from chromadb.utils import embedding_functions
            sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name="all-mpnet-base-v2"  # Match the model used in build script
            )
            
            self.client = chromadb.PersistentClient(path=self.db_path)
            self.collection = self.client.get_collection(
                name="phenotypes",
                embedding_function=sentence_transformer_ef
            )
            logger.info("Vector search initialized successfully with all-mpnet-base-v2")
        except Exception as e:
            logger.error(f"Failed to initialize vector search: {e}")
            raise
    
    def _expand_query_with_synonyms(self, query: str) -> str:
        """Expand query with medical synonyms for better semantic matching."""
        query_lower = query.lower().strip()
        
        # Check if query matches any of our known synonyms
        for key, synonyms in self.synonyms.items():
            if query_lower == key.lower() or query_lower in [syn.lower() for syn in synonyms]:
                # Create an expanded query with the original term plus synonyms
                expanded_terms = [query] + [key] + synonyms
                # Remove duplicates while preserving order
                unique_terms = []
                seen = set()
                for term in expanded_terms:
                    if term.lower() not in seen:
                        unique_terms.append(term)
                        seen.add(term.lower())
                return ' '.join(unique_terms)
        
        return query
    
    def search(
        self, 
        query: str, 
        similarity_threshold: float = 0.25,
        group_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for phenotypes using exact name matching first, then semantic similarity.
        
        Args:
            query: Search query text
            similarity_threshold: Minimum similarity score (0.0 to 1.0)
            group_filter: Optional filter by phenotype group
            
        Returns:
            List of matching phenotypes with similarity scores
        """
        if not self.collection:
            raise RuntimeError("Vector search not initialized")
        

        # Step 1: Try exact name matching first
        exact_matches = self._search_exact_name(query, group_filter)
        
        if exact_matches:
            logger.info(f"Found exact name match for '{query}'")
            # If we found exact matches, search using their descriptions for better semantic results
            combined_results = []
            
            for match in exact_matches:
                # Add the exact match with high score
                combined_results.append({
                    "id": match["id"],
                    "description": match["description"],
                    "group": match["group"],
                    "score": 1.0
                })
                
                # Now search using the description to find related phenotypes
                semantic_results = self._search_semantic(
                    match["description"], 
                    similarity_threshold,
                    group_filter,
                    exclude_ids=[match["id"]]  # Don't include the exact match again
                )
                combined_results.extend(semantic_results)
            
            return combined_results
        else:
            # Step 2: Fall back to semantic search with synonym expansion
            expanded_query = self._expand_query_with_synonyms(query)
            if expanded_query != query:
                logger.info(f"Expanded query '{query}' to '{expanded_query}'")
            return self._search_semantic(expanded_query, similarity_threshold, group_filter)
    
    def _search_exact_name(self, query: str, group_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for exact name matches."""
        where_clause = {"id": query}
        if group_filter:
            where_clause["group"] = group_filter
            
        try:
            results = self.collection.get(
                where=where_clause,
                include=["metadatas"]
            )
            
            exact_matches = []
            for i, metadata in enumerate(results['metadatas']):
                exact_matches.append({
                    "id": results['ids'][i],
                    "description": metadata["description"],
                    "group": metadata["group"]
                })
            
            return exact_matches
        except Exception as e:
            logger.error(f"Exact name search error: {str(e)}")
            return []
    
    def _search_semantic(
        self, 
        query: str, 
        similarity_threshold: float, 
        group_filter: Optional[str] = None,
        exclude_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Perform semantic similarity search."""
        where_clause = None
        if group_filter:
            where_clause = {"group": group_filter}
        
        try:
            results = self.collection.query(
                query_texts=[query],
                n_results=1000,  # Get large number to apply similarity threshold filtering
                where=where_clause,
                include=["metadatas", "distances"]
            )
        except Exception as e:
            logger.error(f"ChromaDB query error: {str(e)}")
            raise
        
        hits = results['ids'][0]
        metadatas = results['metadatas'][0]
        distances = results['distances'][0]
        
        exclude_ids = exclude_ids or []
        filtered_results = []
        
        for i, hit_id in enumerate(hits):
            if hit_id in exclude_ids:
                continue
                
            similarity = 1 - distances[i]
            if similarity >= similarity_threshold:
                filtered_results.append({
                    "id": hit_id,
                    "description": metadatas[i]["description"],
                    "group": metadatas[i]["group"],
                    "score": round(similarity, 4)
                })
        
        logger.info(f"Semantic search for '{query}' returned {len(filtered_results)} results")
        return filtered_results
    
    def get_available_groups(self) -> List[str]:
        """Get list of available phenotype groups."""
        if not self.collection:
            raise RuntimeError("Vector search not initialized")
        
        # Get all metadata and extract unique groups
        results = self.collection.get(include=["metadatas"])
        groups = set()
        for metadata in results['metadatas']:
            if 'group' in metadata:
                groups.add(metadata['group'])
        
        return sorted(list(groups))

# Global instance
vector_search = None

def get_vector_search() -> PhenotypeVectorSearch:
    """Get or create the global vector search instance."""
    global vector_search
    if vector_search is None:
        # Use absolute path to ensure we find the DB regardless of working directory
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        default_db_path = os.path.join(project_root, 'chroma_db')
        db_path = os.getenv('VECTOR_DB_PATH', default_db_path)
        vector_search = PhenotypeVectorSearch(db_path)
    return vector_search
