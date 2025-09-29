# NLP-Query-Engine-for-Employee-Data

# NLP-Query-Engine

## Project Overview

This project implements a Natural Language Processing (NLP) Query Engine designed to interact with both **structured** (SQL database) and **unstructured** (documents/RAG) data sources. The core goal is to provide a unified, schema-agnostic interface that allows users to query employee data using plain English, eliminating the need to write complex SQL or search through large document repositories.

---

## 🎯 Use Case & Purpose

This project is ideal for:

- 💻 **AI Engineering Demos** showcasing a full-stack RAG system with dynamic schema integration.
- 🏢 **Enterprise Search Solutions** where users need to query both structured employee records and unstructured policy documents.
- ⚙️ **Testing and Learning** advanced RAG (Retrieval-Augmented Generation) architectures using Sentence-Transformers and FAISS.
- 🎓 **Educational Demos** in database schema introspection and NL-to-SQL generation.

> ⚠️ **Disclaimer:** This system uses local, open-source models (Sentence-Transformers) and is highly configurable. Performance relies heavily on the quality of the model and the structured data schema.

---

## ✨ Key Features

* **Dynamic Schema Discovery:** Connects to a database (PostgreSQL/MySQL) and automatically discovers table, column, and relationship metadata using **SQLAlchemy** (see `schema_discovery.py`).
* **Natural Language to SQL:** Generates suggested SQL queries from English questions, adapting to the discovered schema.
* **Unstructured Data Indexing (RAG):**
    * Supports ingestion of documents (PDF, DOCX, TXT) via a multi-threaded, asynchronous process.
    * Uses **Sentence-Transformers** for generating high-quality vector embeddings.
    * Employs **FAISS** for highly performant vector indexing and semantic search.
* **Performance Optimization:** Includes in-memory query caching, request metrics, and a dedicated health check endpoint.

---

## 🛠 Tech Stack

| Category | Technology | Purpose |
| :--- | :--- | :--- |
| **Backend Framework** | Python / Flask | High-performance, lightweight API server. |
| **Database ORM** | SQLAlchemy | Abstract database interaction and dynamic schema inspection. |
| **Vector Search** | FAISS | Efficient indexing and retrieval of document embeddings. |
| **NLP/Embeddings** | Sentence-Transformers | Generating vector representations for semantic search (RAG). |
| **ML/Utilities** | PyTorch, NumPy, scikit-learn | Core dependencies for model execution and utility functions. |
| **Client** | HTML/JavaScript (via `index.html`) | Simple web interface for interaction. |

---

## 🚀 Setup and Installation

### Prerequisites

* Python 3.8+
* A target database (e.g., PostgreSQL or MySQL) is highly recommended for full functionality.

### 1. Clone the repository

```bash
git clone <repository_url>
cd project/backend
```
### 2.Set up a Virtual Environment and Install Dependencies
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
pip install -r requirements.txt
```
### 3. Running the Application
```
python app.py
```
