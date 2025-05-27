#!/usr/bin/env python3
"""
Script to build vector embeddings from the Phenotypes table using ChromaDB.
"""

import os
import logging
import chromadb
from chromadb.config import Settings
import sqlalchemy
from dataregistry.api.db import DataRegistryReadWriteDB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_phenotypes_from_db():
    """Fetch phenotypes from the database."""
    db = DataRegistryReadWriteDB()
    engine = db.get_engine()
    
    query = "SELECT id, name, description, `group` FROM Phenotypes"
    
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(query))
        phenotypes = []
        for row in result:
            phenotypes.append({
                'id': str(row[0]),
                'name': row[1],
                'description': row[2],
                'group': row[3]
            })
    
    logger.info(f"Fetched {len(phenotypes)} phenotypes from database")
    return phenotypes

def build_chromadb_collection(db_path: str = "./chroma_db"):
    """Build ChromaDB collection from phenotypes data."""
    
    # Initialize ChromaDB client
    client = chromadb.PersistentClient(path=db_path)
    
    # Use a better embedding function for semantic search
    from chromadb.utils import embedding_functions
    sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-mpnet-base-v2"  # Better than default all-MiniLM-L6-v2
    )
    
    # Create or get collection
    collection_name = "phenotypes"
    try:
        collection = client.delete_collection(name=collection_name)
        logger.info(f"Deleted existing collection: {collection_name}")
    except Exception:
        pass
    
    collection = client.create_collection(
        name=collection_name,
        embedding_function=sentence_transformer_ef
    )
    logger.info(f"Created collection: {collection_name} with all-mpnet-base-v2")
    
    # Fetch phenotypes
    phenotypes = fetch_phenotypes_from_db()
    
    # Prepare data for ChromaDB
    ids = []
    documents = []
    metadatas = []
    
    for pheno in phenotypes:
        ids.append(pheno['id'])
        # Combine name and description for better semantic search
        documents.append(f"{pheno['name']}: {pheno['description']}")
        metadatas.append({
            'name': pheno['name'],
            'description': pheno['description'],
            'group': pheno['group']
        })
    
    # Add to collection
    collection.add(
        ids=ids,
        documents=documents,
        metadatas=metadatas
    )
    
    logger.info(f"Added {len(ids)} documents to ChromaDB collection")
    return collection

def test_search(collection):
    """Test the search functionality."""
    logger.info("Testing search functionality:")
    test_queries = ["diabetes", "brain volume", "smoking"]
    
    for query in test_queries:
        logger.info(f"\nSearching for: '{query}'")
        results = collection.query(
            query_texts=[query],
            n_results=3,
            include=["metadatas", "distances"]
        )
        
        hits = results['ids'][0]
        metadatas = results['metadatas'][0]
        distances = results['distances'][0]
        
        for i, hit_id in enumerate(hits):
            similarity = 1 - distances[i]
            logger.info(f"  {i+1}. {metadatas[i]['name']} (score: {similarity:.3f})")
            logger.info(f"     Description: {metadatas[i]['description']}")

if __name__ == "__main__":
    # Build the ChromaDB collection
    collection = build_chromadb_collection()
    
    # Test the search functionality
    test_search(collection)