import psycopg2
import requests
from bs4 import BeautifulSoup

# Database connection details
source_db = {
    "dbname": "Aggre",
    "user": "postgres",
    "password": "siddh2003",
    "host": "localhost",
    "port": "5432"
}

destination_db = {
    "dbname": "Aggre",
    "user": "postgres",
    "password": "siddh2003",
    "host": "localhost",
    "port": "5432"
}

def reset_destination_table():
    """Check if the destination table exists; if so, delete and recreate it."""
    try:
        conn = psycopg2.connect(**destination_db)
        cur = conn.cursor()
        
        # Drop the table if it exists
        cur.execute("DROP TABLE IF EXISTS sample")
        
        # Create the table again with desired columns
        cur.execute("""
            CREATE TABLE sample (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                original_url TEXT NOT NULL,
                image_url TEXT
            )
        """)
        
        conn.commit()
        conn.close()
        print("Destination table 'sample' has been reset.")
    except Exception as e:
        print(f"Error resetting destination table: {e}")

def fetch_urls():
    """Fetch new article URLs from the source table."""
    try:
        conn = psycopg2.connect(**source_db)
        cur = conn.cursor()
        cur.execute("SELECT url FROM news")
        urls = cur.fetchall()
        conn.close()
        urls = [url[0] for url in urls]
        print(f"Fetched URLs: {urls}")  # Debug print
        return urls
    except Exception as e:
        print(f"Error fetching URLs: {e}")
        return []

def extract_article_content(url):
    """Extract the headline, content, original URL, and image URL from an article URL."""
    try:
        print(f"Fetching article from: {url}")  # Debug print
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Customize these selectors based on the website structure
        headline = soup.find('h1').get_text(strip=True)
        content = ' '.join([p.get_text(strip=True) for p in soup.find_all('p')])
        
        # Original URL (to be stored)
        original_url = url
        
        # Find the first image URL (customize selector as necessary)
        image_url = soup.find('img')['src'] if soup.find('img') else None
        
        return headline, content, original_url, image_url
    except Exception as e:
        print(f"Error fetching article from {url}: {e}")
        return None, None, None, None

def store_article(headline, content, original_url, image_url):
    """Store the headline, content, original URL, and image URL into the destination table."""
    try:
        conn = psycopg2.connect(**destination_db)
        cur = conn.cursor()
        cur.execute("INSERT INTO sample (title, content, original_url, image_url) VALUES (%s, %s, %s, %s)", 
                    (headline, content, original_url, image_url))
        conn.commit()
        conn.close()
        print(f"Stored article titled '{headline}' from {original_url}")  # Debug print
    except Exception as e:
        print(f"Error storing article: {e}")

def main():
    reset_destination_table()  # Reset the table before starting
    urls = fetch_urls()
    for url in urls:
        headline, content, original_url, image_url = extract_article_content(url)
        if headline and content:
            store_article(headline, content, original_url, image_url)
        else:
            print(f"Skipped storing for URL: {url}")  # Debug print for skipped articles

if __name__ == "__main__":
    main()
