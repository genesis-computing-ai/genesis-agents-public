from annoy import AnnoyIndex
import csv, json

def load_embeddings_from_csv(csv_file_path):
    embeddings = []
    filenames = []
    with open(csv_file_path, mode='r', newline='') as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)  # Skip the header
        for row in reader:
            embedding = [float(x) for x in row[1].strip("[]").split(", ")]
            filename = row[0]
            filenames.append(filename)
            embeddings.append(embedding)
    return embeddings, filenames

def create_annoy_index(embeddings, n_trees=10):
    dimension = len(embeddings[0])  # tAssuming all embeddings have the same dimension
    index = AnnoyIndex(dimension, 'angular')  # Using angular distance
    for i, embedding in enumerate(embeddings):
        print("indexing #", i)
        index.add_item(i, embedding)
    index.build(n_trees)
    return index

csv_file_path = './tmp/embedding_out_full.csv'  # Adjust if necessary
embeddings, filenames = load_embeddings_from_csv(csv_file_path)

print("indexing ",len(embeddings)," embeddings...")

# Create the Annoy index
annoy_index = create_annoy_index(embeddings)

# Save the index to a file
index_file_path = './tmp/embeddings_full.ann'
annoy_index.save(index_file_path)

with open('./tmp/mappings_full.json', 'w') as f:
    json.dump(filenames, f)

print(f"Annoy index saved to {index_file_path}")

