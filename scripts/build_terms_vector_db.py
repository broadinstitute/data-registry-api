#!/usr/bin/env python3
"""
Script to build vector embeddings from the human_lance_export.csv file using ChromaDB.
Creates a 'terms' collection for semantic search of biological terms.
"""

import os
import logging
import chromadb
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_terms_from_csv(csv_path: str = "human_lance_export.csv"):
    """Fetch terms from CSV file."""
    df = pd.read_csv(csv_path)
    
    terms_list = []
    for idx, row in df.iterrows():
        terms_list.append({
            "id": f"term_{idx}",
            "term": row['term'],
            "phenotype": row['phenotype'],
            "gene_set": row['gene_set'], 
            "source": row['source'],
            "beta_uncorrected": str(row['beta_uncorrected'])
        })
    
    logger.info(f"Fetched {len(terms_list)} terms from CSV")
    return terms_list

def build_terms_chromadb_collection(db_path: str = "./chroma_db"):
    """Build ChromaDB terms collection from the CSV data."""
    
    # Initialize ChromaDB client
    client = chromadb.PersistentClient(path=db_path)
    
    # Use the same embedding function as phenotypes collection
    from chromadb.utils import embedding_functions
    sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-mpnet-base-v2"
    )
    
    # Create or get collection
    collection_name = "terms"
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
    
    # Fetch terms
    terms = fetch_terms_from_csv()
    
    # Prepare data for ChromaDB
    ids = []
    documents = []
    metadatas = []
    
    for term in terms:
        ids.append(term['id'])
        # Use the term text for semantic search
        documents.append(term['term'])
        metadatas.append({
            'term': term['term'],
            'phenotype': term['phenotype'],
            'gene_set': term['gene_set'],
            'source': term['source'],
            'beta_uncorrected': term['beta_uncorrected']
        })
    
    # Add to collection in batches to avoid memory issues
    batch_size = 1000
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i+batch_size]
        batch_documents = documents[i:i+batch_size]
        batch_metadatas = metadatas[i:i+batch_size]
        
        collection.add(
            ids=batch_ids,
            documents=batch_documents,
            metadatas=batch_metadatas
        )
        logger.info(f"Added batch {i//batch_size + 1} ({len(batch_ids)} documents)")
    
    logger.info(f"Added {len(ids)} total documents to ChromaDB terms collection")
    return collection

def test_terms_search(collection):
    """Test the terms search functionality."""
    logger.info("Testing terms search functionality:")
    test_queries = ["heart", "diabetes", "liver", "brain"]
    
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
            logger.info(f"  {i+1}. {hit_id} (score: {similarity:.3f})")
            logger.info(f"     Term: {metadatas[i]['term']}")
            logger.info(f"     Phenotype: {metadatas[i]['phenotype']}")
            logger.info(f"     Source: {metadatas[i]['source']}")

if __name__ == "__main__":
    # Build the ChromaDB terms collection
    collection = build_terms_chromadb_collection()
    
    # Test the search functionality
    test_terms_search(collection)