import singlestoredb as s2
from urllib.parse import urlparse, urlunparse
import json
import os
import requests

def get_db_connection():
    conn = s2.connect(os.getenv("SINGLESTORE_KEY"))
    return conn

def add_source(conn, title: str, url: str, paper_id: int):
    url = clean_url(url)
    cursor = conn.cursor()

    # Check if the URL already exists
    query = "SELECT id FROM Sources WHERE url = %s LIMIT 1"
    cursor.execute(query, (url,))
    result = cursor.fetchone()

    if result:
        source_id = result[0]
    else:
        embedding = get_embedding_from_source(url, title)

        # Convert the embedding (list of floats) to a comma-separated string
        embedding_json = json.dumps(embedding)

        insert_query = """
            INSERT INTO Sources (title, url, embedding
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (title, url, embedding_json))

        # Get the newly inserted source's ID
        source_id = cursor.lastrowid
        print(f"Inserted new source with ID: {source_id}")

    print(source_id)

    # Link the source to the paper
    insert_source_query = """
        INSERT INTO Papers_Sources (paper_id, source_id)
        VALUES(%s, %s)
    """
    cursor.execute(insert_source_query, (paper_id, source_id))
    conn.commit()
    cursor.close()

    return source_id




#adds paper to papers table with the given title and text, returns the id of the new entry
def add_paper(conn, title: str, text: str, owner: str):
	cursor = conn.cursor()
	embedding = get_embedding_from_paper(title, text)
	print("made embedding")
	query = """
		INSERT INTO Papers (title, text, embedding, owner)
		VALUES (%s, %s, %s, %s)
	"""

	embedding_json = json.dumps(embedding)
	cursor.execute(query, (title, text, embedding_json, owner))
	print("executed cursor")
	paper_id = cursor.lastrowid
	conn.commit()
	cursor.close()

	return paper_id

#updates paper paper_id with title, text, and embedding
def update_paper(conn, paper_id: int, title: str, text: str):
    cursor = conn.cursor()
    embedding = get_embedding_from_paper(title, text)

    query = """
        UPDATE Papers
        SET title = %s, text = %s, embedding = %s
        WHERE id = %s
    """

    embedding_json = json.dumps(embedding)
    cursor.execute(query, (title, text, embedding_json, paper_id))
    conn.commit()
    cursor.close()

#searches for 50 most similar papers, using paper embeddings, and returns the list sorted in descending order TODO need to get this to return scores as well
def get_similar_papers(conn, paper_id: int):
    cursor = conn.cursor()

    query = """
        SELECT embedding 
        FROM papers 
        WHERE id = %s
    """
    cursor.execute(query, (paper_id,))
    paper_embedding = cursor.fetchone()

    if not paper_embedding:
        cursor.close()
        raise ValueError(f"Paper with id {paper_id} not found")

    vector_search_query = """
        SET @query_vec = (?::VECTOR(768));

        SELECT id, title, text <*> @query_vec AS score
        FROM papers
        WHERE id != ?
        ORDER BY score DESC
        LIMIT 50;
    """

    # Execute the vector similarity search query
    cursor.execute(vector_search_query, (paper_embedding[0], paper_id))
    similar_papers = cursor.fetchall()

    # Close the cursor
    cursor.close()

    return similar_papers

#returns a list of paper_ids that are connected to a certain source source_id
def get_source_papers(conn, source_id: int):
	cursor = conn.cursor()

	query = """ 
		SELECT paper_id
		FROM Papers_Sources
		WHERE source_id = %s;
	"""

	cursor.execute(query, (source_id,))
	papers = cursor.fetchall()
	cursor.close()

	return papers

# Retrieve all papers from the Papers table
def get_all_papers(conn):
    cursor = conn.cursor()

    query = """
        SELECT id, title, text, owner
        FROM Papers
    """
    cursor.execute(query)
    papers = cursor.fetchall()
    cursor.close()

    # Format papers into a list of dictionaries
    all_papers = [
        {"id": paper[0], "title": paper[1], "text": paper[2], "owner": paper[3]}
        for paper in papers
    ]

    return all_papers


# Retrieve all sources linked to a specific paper
def get_paper_sources(conn, paper_id):
    cursor = conn.cursor()

    query = """
        SELECT s.id, s.title, s.url
        FROM Sources s
        JOIN Papers_Sources ps ON s.id = ps.source_id
        WHERE ps.paper_id = %s
    """
    cursor.execute(query, (paper_id,))
    sources = cursor.fetchall()
    cursor.close()

    # Format sources into a list of dictionaries
    paper_sources = [
        {"id": source[0], "title": source[1], "url": source[2]}
        for source in sources
    ]

    return paper_sources


# Retrieve all papers owned by a specific user
def get_user_papers(conn, user: str):
    cursor = conn.cursor()

    query = """
        SELECT id, title, text
        FROM Papers
        WHERE owner = %s
    """
    cursor.execute(query, (user,))
    user_papers = cursor.fetchall()
    cursor.close()

    # Format papers into a list of dictionaries
    papers = [
        {"id": paper[0], "title": paper[1], "text": paper[2]}
        for paper in user_papers
    ]

    return papers




# utility functions NEED TODO
def clean_url(url: str) -> str:
    # Parse the URL into its components
    parsed_url = urlparse(url)
    
    # Rebuild the URL without query parameters, fragments, or trailing slashes
    cleaned_url = urlunparse((
        parsed_url.scheme,       # http or https
        parsed_url.netloc.lower(),  # Domain name (lowercased for consistency)
        parsed_url.path.rstrip('/'),  # Path, with trailing slash removed
        '',                      # No params (not typically used in URLs)
        '',                      # Remove query string (parameters)
        ''                       # Remove fragment (the part after '#')
    ))
    
    return cleaned_url

def get_embedding_from_source(url: str, title: str) -> [float]:
	return get_embeddings(title)

def get_embedding_from_paper(title: str, text: str):
	return get_embeddings(title + " " + text)



# Function to split text into chunks
def split_text(text, max_length=512):
    words = text.split()
    chunks = []
    current_chunk = []
    current_length = 0
    
    for word in words:
        word_tokens = tokenizer.tokenize(word)
        word_length = len(word_tokens)
        
        if current_length + word_length > max_length:
            chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_length = word_length
        else:
            current_chunk.append(word)
            current_length += word_length
    
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    return chunks

# Function to get embeddingss
def get_embeddings(text):
    url = "http://147.182.163.168:6969/embed"
    data = {"text": text}
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, data=json.dumps(data), headers=headers)
    print(response.json()['embeddings'])
    return response.json()['embeddings']

if __name__ == '__main__':
    conn = get_db_connection()
    print(conn)
    print(get_paper_sources(conn, 2251799813685254))
    conn.close()
