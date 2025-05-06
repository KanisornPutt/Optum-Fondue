# OPTUM - LLM Assisted Analysis for Traffy Fondue
OPTUM is an AI-powered language agent designed to streamline the analysis of public complaints from the Traffy Fondue platform, a widely-used system in Bangkok for reporting municipal issues. Traditional keyword search is ineffective with unstructured, citizen-generated content, so we built a system leveraging advanced NLP techniques like contextual embedding and Retrieval-Augmented Generation (RAG) to understand, retrieve, and summarize data effectively.

## Project Summary
OPTUM enables real-time insights into public concerns by embedding semantic meaning into reports and queries. It helps government officials and city planners detect patterns, spot trends, and take action—without manual filtering.
- Embeds reports and queries into vectors for semantic understanding.
- Retrieves relevant entries using Qdrant vector search.
- Summarizes insights with LLM (Gemini 2.0 Flash).
- Enhances transparency and speeds up public service responses.

## Tech Stack
- **Dataset**	700K+ reports from Traffy Fondue, plus 2,000+ via API
- **Embedding** Model	Fine-tuned clicknext/phayathaibert  **(KanisornPutta/TrentIsNotLeavingBERT)**
- **Vector** **DB**	Qdrant
- **Backend**	FastAPI
- **Metadata DB**	SQLite
- **Visualization**	Web interface with maps, filters, and charts
- **LLM**	Gemini 2.0 Flash (for summarization)

## Features
### Automated ETL Pipeline (via Airflow DAGs)
- fetch_data: Pulls recent complaints from Traffy API
- clean_data: Prepares data for embedding
- embedding_data: Converts text to semantic vectors
- save_to_qdrant: Uploads to Qdrant vector DB

### Query Handling
- User query embedded
- Similar tickets retrieved from Qdrant
- Gemini LLM summarizes results
- Metadata (e.g., ticket ID, address) fetched for UI display

### Data Visualization
Ticket counts by type and time
Heatmap, scatter map, and clustering
Intuitive web interface for filtering and exploration

## References
- Traffy Fondue API: https://www.traffy.in.th
- Qdrant Vector DB: https://qdrant.tech
- Gemini 2.0 Flash LLM: Google DeepMind
