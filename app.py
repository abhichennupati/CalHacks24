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

# Running the Flask app
if __name__ == '__main__':
    app.run(debug=True)