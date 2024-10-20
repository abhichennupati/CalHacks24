from flask import Flask, request, jsonify
from data_helpers import get_similar_papers, get_source_papers, get_db_connection

app = Flask(__name__)

# API endpoint for retrieving similar papers
@app.route('/get_similar_papers', methods=['POST'])
def api_get_similar_papers():
    data = request.json
    paper_id = data.get('paper_id')

    if paper_id is None:
        return jsonify({"error": "Missing paper_id in request"}), 400

    try:
        conn = get_db_connection()
        similar_papers = get_similar_papers(conn, paper_id)
        conn.close()

        # Format the response
        papers = [
            {"id": paper["id"], "title": paper["title"], "text": paper["text"], "score": paper["score"]}
            for paper in similar_papers
        ]

        return jsonify({"papers": papers})

    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": "An error occurred"}), 500

# API endpoint for retrieving papers linked to a source
@app.route('/get_source_paper_links', methods=['POST'])
def api_get_source_paper_links():
    data = request.json
    source_id = data.get('source_id')

    if source_id is None:
        return jsonify({"error": "Missing source_id in request"}), 400

    try:
        conn = get_db_connection()
        linked_papers = get_source_papers(conn, source_id)
        conn.close()

        # Format the response
        papers = [{"paper_id": paper["paper_id"]} for paper in linked_papers]

        return jsonify({"papers": papers})

    except Exception as e:
        return jsonify({"error": "An error occurred"}), 500

@app.route('/add_paper', methods=['POST'])
def api_add_paper():
    data = request.json
    text = data.get('text')
    title = data.get('title')
    owner = data.get('owner')

    if title is None or text is None or owner is None:
        return jsonify({'error': "missing field"}), 400

    try:
        conn = get_db_connection()
        paper_id = add_paper(conn, title, text, owner)
        conn.close()
        return jsonify({'id': paper_id})
    except Exception as e:
        conn.close()
        return jsonify({"error": "An error occurred"}), 500

@app.route('/add_paper', methods=['POST'])
def api_update_paper():
    data = request.json
    paper_id = data.get('id')
    text = data.get('text')
    title = data.get('title')

    if title is None or text is None or paper_id is None:
        return jsonify({'error': "missing field"})

    try:
        conn = get_db_connection()
        update_paper(conn, paper_id, title, text)
        conn.close()
    except Exception as e:
        conn.close()
        return jsonify({"error": "An error occurred"}), 500

@app.route('/add_source', methods=['POST'])
def api_add_source():
    data = request.json
    paper_id = data.get('paper_id')
    url = data.get('url')
    title = data.get('title')

    if title is None or url is None or paper_id is None:
        return jsonify({'error': "missing field"}), 400

    try:
        conn = get_db_connection()
        source_id = add_source(conn, title, url, paper_id)
        conn.close()
        return jsonify({'id': source_id})
    except Exception as e:
        conn.close()
        return jsonify({"error": "An error occurred"}), 500

# Running the Flask app
if __name__ == '__main__':
    app.run(debug=True)