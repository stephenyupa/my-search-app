# app.py
import streamlit as st
import tempfile
import os
from db import init_db, ingest_csv_in_chunks, ingest_txt_in_chunks, search_records

def main():
    st.title("Large File Search App")

    st.write("""
        This app can handle large CSV/TXT files in chunks.
        After uploading, you'll see a small preview of the data,
        and you can repeatedly search for partial text matches.
    """)

    # 1) Initialize or connect to the DB
    engine = init_db()

    # 2) File Uploader (CSV or TXT)
    uploaded_file = st.file_uploader("Upload your CSV or TXT file", type=["csv", "txt"])
    if uploaded_file is not None:
        # Optional: Clear existing records if user wants a fresh start
        if st.checkbox("Clear existing data before ingest? (Warning: This removes old data)", value=False):
            with engine.connect() as conn:
                conn.execute("DELETE FROM records")

        # Write uploaded file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(uploaded_file.read())
            tmp.flush()
            temp_filepath = tmp.name

        # Decide how to handle CSV vs TXT
        _, file_extension = os.path.splitext(uploaded_file.name)
        file_extension = file_extension.lower()

        st.info("Ingesting file in chunks. This may take a while for very large files.")
        with st.spinner("Ingesting..."):
            preview_data = None
            if file_extension == ".csv":
                preview_data = ingest_csv_in_chunks(engine, temp_filepath)
            elif file_extension == ".txt":
                preview_data = ingest_txt_in_chunks(engine, temp_filepath)
            else:
                st.error("Unsupported file type. Please upload a .csv or .txt")
                return

        st.success("File uploaded and ingested into the database successfully!")

        # Show a small preview (first ~50 rows)
        if preview_data is not None and not preview_data.empty:
            st.write("Preview of the first rows in your file:")
            st.dataframe(preview_data)
        else:
            st.write("No data to preview or file was empty.")

    st.subheader("Search the Database")

    # 3) Multi-token partial substring search
    query_str = st.text_input("Enter partial text (e.g., 'cas ford lie'):")
    if st.button("Search"):
        if query_str.strip():
            results = search_records(engine, query_str.strip())
            st.write(f"Found {len(results)} result(s).")

            # If many results, show the first 200 for performance reasons
            max_to_show = 200
            for row in results[:max_to_show]:
                st.write(row)
            if len(results) > max_to_show:
                st.write("... (Showing the first 200 results only)")
        else:
            st.warning("Please enter a valid search term.")

if __name__ == "__main__":
    main()
