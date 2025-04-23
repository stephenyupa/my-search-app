# db.py
import pandas as pd
from sqlalchemy import create_engine, text

def init_db(db_uri="sqlite:///records.db"):
    """
    Create (or connect to) a SQLite database and ensure
    a 'records' table exists with 'id' and 'line' columns.
    """
    engine = create_engine(db_uri)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                line TEXT
            )
        """))
    return engine

def ingest_csv_in_chunks(engine, csv_path, chunksize=50000):
    """
    Ingest a large CSV file into the 'records' table in chunks.
    Each row is stored as a single line (TEXT).
    """
    # We'll store the first chunk's data in memory to display a small preview
    first_chunk_data = None

    # Stream the CSV in chunks
    reader = pd.read_csv(
        csv_path,
        header=None,        # treat the CSV as no headers
        dtype=str,          # store everything as string
        na_filter=False,    # don't parse NaNs
        chunksize=chunksize
    )

    for i, chunk in enumerate(reader):
        chunk.columns = ["line"]  # single column called 'line'
        if i == 0:
            first_chunk_data = chunk.head(50)  # keep up to 50 lines for preview
        # Append chunk to DB
        chunk.to_sql("records", engine, if_exists="append", index=False)
    return first_chunk_data

def ingest_txt_in_chunks(engine, txt_path, chunksize=50000):
    """
    Ingest a large text file into the 'records' table in chunks of lines.
    """
    first_chunk_data = []
    buffer = []
    count = 0
    chunk_index = 0

    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            buffer.append(line.strip("\n\r"))
            count += 1

            # If we've reached the chunksize, store this chunk in the DB
            if count % chunksize == 0:
                chunk_df = pd.DataFrame({"line": buffer})
                # Save chunk to DB
                chunk_df.to_sql("records", engine, if_exists="append", index=False)
                # Save chunk #0 for preview
                if chunk_index == 0:
                    first_chunk_data = chunk_df.head(50)
                buffer = []
                chunk_index += 1

    # If there are leftover lines in the buffer after the loop ends
    if buffer:
        chunk_df = pd.DataFrame({"line": buffer})
        chunk_df.to_sql("records", engine, if_exists="append", index=False)
        if chunk_index == 0:  # means everything fit into one chunk
            first_chunk_data = chunk_df.head(50)

    return first_chunk_data

def search_records(engine, query_str):
    """
    Perform a multi-token, case-insensitive, partial-substring search.
    Example: "cas ford lie" => must find "cas" AND "ford" AND "lie" in the line.
    """
    # Split user query into tokens (strip whitespace, lowercase, etc.)
    tokens = [t.strip().lower() for t in query_str.split() if t.strip()]
    if not tokens:
        return []

    # Build a WHERE clause with AND across each token:
    # e.g.  WHERE lower(line) LIKE :token0 AND lower(line) LIKE :token1 ...
    conditions = []
    params = {}
    for i, token in enumerate(tokens):
        param_name = f"token{i}"
        conditions.append(f"lower(line) LIKE :{param_name}")
        # wrap each token in %...% for partial substring matching
        params[param_name] = f"%{token}%"

    where_clause = " AND ".join(conditions)
    query = f"SELECT line FROM records WHERE {where_clause}"

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        return [row[0] for row in result]
