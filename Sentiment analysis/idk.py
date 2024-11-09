from transformers import pipeline, AutoTokenizer
import torch
import psycopg2
from psycopg2 import sql

def summarize_and_analyze(text, max_tokens=512):
    # Check if CUDA (GPU) is available
    device = 0 if torch.cuda.is_available() else -1

    # Initialize summarization and sentiment analysis pipelines with the specified device
    summarizer = pipeline("summarization", device=device)
    sentiment_analyzer = pipeline("sentiment-analysis", device=device)

    # Load the tokenizer for the summarization model
    tokenizer = AutoTokenizer.from_pretrained("facebook/bart-large-cnn")

    # Tokenize and split text into manageable chunks of max_tokens tokens
    inputs = tokenizer(text, return_tensors="pt", max_length=max_tokens, truncation=True, padding="longest")
    chunks = [tokenizer.decode(inputs["input_ids"][0, i:i + max_tokens], skip_special_tokens=True) 
              for i in range(0, inputs["input_ids"].size(1), max_tokens)]

    # Summarize each chunk and combine summaries
    summaries = [summarizer(chunk, max_length=1000, min_length=25, do_sample=False, truncation=True)[0]['summary_text'] 
                 for chunk in chunks]
    combined_summary = " ".join(summaries)

    # Run sentiment analysis on the combined summary
    sentiment = sentiment_analyzer(combined_summary)[0]
    
    return {
        "summary": combined_summary,
        "sentiment_label": sentiment['label']
    }

def store_in_database(cursor, title, data, original_url, image_url):
    try:
        # Insert the data into the news_article table
        insert_query = """
        INSERT INTO news_article (title, content, original_url, image_url, score) 
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (title, data["summary"], original_url, image_url, data["sentiment_label"]))

    except Exception as e:
        print("Error during insert:", e)

def get_text():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname="Aggre",
            user="postgres",
            password="siddh2003",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Drop the table if it exists, then create it with additional columns
        cursor.execute("DROP TABLE IF EXISTS news_article;")
        creation_query = """
        CREATE TABLE news_article (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            content TEXT,
            original_url TEXT,
            image_url TEXT,
            score VARCHAR(255)
        )
        """
        cursor.execute(creation_query)
        conn.commit()  # Commit table creation

        # Retrieve title, content, original_url, and image_url from the sample table
        retrieve_cmd = "SELECT title, content, original_url, image_url FROM sample"
        cursor.execute(retrieve_cmd)
        rows = cursor.fetchall()

        # Summarize, analyze, and store each row's text content
        for row in rows:
            title, text, original_url, image_url = row
            res = summarize_and_analyze(text=text)
            store_in_database(cursor, title, res, original_url, image_url)

        # Commit all inserts at once
        conn.commit()

    except Exception as e:
        print("Error:", e)

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    get_text()
