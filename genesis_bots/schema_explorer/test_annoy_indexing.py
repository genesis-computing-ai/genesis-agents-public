import numpy as np
from annoy import AnnoyIndex
import multiprocessing
from multiprocessing import Process, Queue
import signal
import time
import tempfile
import os
import json

# Constants
INDEX_FILE_PATH = './tmp/'
EMBEDDING_SIZE = 3072  # or 128 for testing

def create_annoy_index(embeddings, n_trees=5):
    dimension = len(embeddings[0])
    print(f"[Main] Creating Annoy index (dimension: {dimension})")
    print(f"[Main] Processing {len(embeddings)} embeddings")

    # Verify embeddings are normalized
    norms = [np.linalg.norm(emb) for emb in embeddings]
    print(f"[Main] Embedding norms min/max/mean: {min(norms):.3f}/{max(norms):.3f}/{np.mean(norms):.3f}")

    if any(abs(n - 1.0) > 0.01 for n in norms):
        print("[Main] Some embeddings are not normalized, normalizing...")
        embeddings = [np.array(emb)/(1e-5+np.linalg.norm(emb)) for emb in embeddings]

    index = AnnoyIndex(dimension, 'angular')

    try:
        print("[Main] Adding items to index...")
        for i, embedding in enumerate(embeddings):
            try:
                index.add_item(i, embedding)
                if i % 100 == 0:
                    print(f"[Main] Progress: {i}/{len(embeddings)} embeddings added")
            except Exception as e:
                print(f"[Main] Error on embedding {i}: {e}")
                continue
        
        print(f"[Main] Building index with {n_trees} trees...")
        index.build(n_trees, n_jobs=1)
        print("[Main] Index built successfully!")
    except Exception as e:
        print(f"[Main] Indexing exception: {e}")
        return None

    return index

def make_and_save_index(vectors, bot_id="test"):
    print(f"[Main] Indexing {len(vectors)} vectors...")

    if len(vectors) == 0:
        print("[Main] No vectors provided, creating dummy index")
        vectors = [[0.0] * EMBEDDING_SIZE]
        metadata = ['empty_index']
    else:
        metadata = [f"vector_{i}" for i in range(len(vectors))]

    try:
        annoy_index = create_annoy_index(vectors)
        if annoy_index is None:
            return None, None
    except Exception as e:
        print(f'[Main] Error on create_index: {e}')
        return None, None

    print("[Main] Saving index to file...")

    # Create directory if it doesn't exist
    if not os.path.exists(INDEX_FILE_PATH):
        os.makedirs(INDEX_FILE_PATH)

    # Save with timestamp filename
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    index_file_name = f'index_{bot_id}_{timestamp}.ann'
    meta_file_name = f'metadata_{bot_id}_{timestamp}.json'

    try:
        annoy_index.save(os.path.join(INDEX_FILE_PATH, index_file_name))
    except Exception as e:
        print(f'[Main] Cannot save Annoy index: {e}')
        return None, None

    print(f"[Main] Saving metadata to {meta_file_name}")
    with open(os.path.join(INDEX_FILE_PATH, meta_file_name), 'w') as f:
        json.dump(metadata, f)

    # Save with default filename
    default_index_name = f'latest_cached_index_{bot_id}.ann'
    default_meta_name = f'latest_cached_metadata_{bot_id}.json'
    
    annoy_index.save(os.path.join(INDEX_FILE_PATH, default_index_name))
    with open(os.path.join(INDEX_FILE_PATH, default_meta_name), 'w') as f:
        json.dump(metadata, f)

    # Save embedding size
    with open(os.path.join(INDEX_FILE_PATH, 'index_size.txt'), 'w') as f:
        f.write(str(len(vectors[0])))

    return annoy_index, metadata

def test_index_search(index, test_vector, metadata, n_neighbors=5):
    print("\n[Test] Searching for nearest neighbors...")
    nearest_ids = index.get_nns_by_vector(test_vector, n_neighbors, include_distances=True)
    print(f"[Test] Found {len(nearest_ids[0])} nearest neighbors")
    for idx, dist in zip(nearest_ids[0], nearest_ids[1]):
        print(f"[Test] ID: {idx}, Name: {metadata[idx]}, Distance: {dist:.4f}")

def main():
    # Test configurations
    test_configs = [
        {"name": "Small", "n_vectors": 1000, "dimension": 128, "n_trees": 5, "bot_id": "test_small"},
        {"name": "Large", "n_vectors": 100, "dimension": 3072, "n_trees": 5, "bot_id": "test_large"}
    ]

    for config in test_configs:
        print(f"\n{'='*50}")
        print(f"Testing {config['name']} Vectors Configuration:")
        print(f"Vectors: {config['n_vectors']}, Dimension: {config['dimension']}, Trees: {config['n_trees']}")
        print(f"{'='*50}\n")
        
        # Generate test vectors
        vectors = np.random.randn(config['n_vectors'], config['dimension'])
        vectors = vectors / np.linalg.norm(vectors, axis=1)[:, np.newaxis]
        vectors = vectors.tolist()

        try:
            start_time = time.time()
            index, metadata = make_and_save_index(vectors, bot_id=config['bot_id'])
            build_time = time.time() - start_time
            print(f"\n[Timing] Index build took {build_time:.2f} seconds")
            
            if index:
                test_vector = np.random.randn(config['dimension'])
                test_vector = (test_vector / np.linalg.norm(test_vector)).tolist()
                
                start_time = time.time()
                test_index_search(index, test_vector, metadata)
                search_time = time.time() - start_time
                print(f"[Timing] Search took {search_time:.4f} seconds")
                
        except Exception as e:
            print(f"\n[Main] Error during test: {e}")

if __name__ == "__main__":
    main()