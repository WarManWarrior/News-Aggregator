import axios from "axios";
import pg from "pg";

const client = new pg.Client({
    user: 'postgres',
    host: 'localhost',
    database: 'Aggre',
    password: 'siddh2003',
    port: 5432,
});

client.connect();

async function createTable() {
    const createTableQuery = `
    CREATE TABLE news (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255),
        content TEXT,
        source_name VARCHAR(255),
        published_at TIMESTAMP,
        url VARCHAR(255),
        img VARCHAR(255)
    );
    `;

    await client.query(createTableQuery);
    console.log('Table created or already exists');
}

async function createTempTable() {
    const createTempTableQuery = `
    CREATE TEMPORARY TABLE temp_news (
        title VARCHAR(255),
        content TEXT,
        source_name VARCHAR(255),
        published_at TIMESTAMP,
        url VARCHAR(255),
        img VARCHAR(255)
    );
    `;
    await client.query(createTempTableQuery);
    console.log('Temporary table for news created');
}

async function clearTable() {
    const deleteQuery = `DROP TABLE IF EXISTS news;`; // Drop the table if it exists
    await client.query(deleteQuery);
    console.log('Existing news table dropped');
}

async function fetchNews() {
    const apiKey = '7c8ad1cc99f5418787af6f7b8341c915';
    const url = `https://newsapi.org/v2/everything?q=bitcoin&apiKey=${apiKey}`;

    try {
        const response = await axios.get(url);
        return response.data.articles;
    } catch (error) {
        console.error('Error fetching news', error);
        return [];
    }
}

async function insertTempNews(articles) {
    const insertQuery = `
    INSERT INTO temp_news (title, content, source_name, published_at, url, img)
    VALUES ($1, $2, $3, $4, $5, $6);
    `;

    for (const article of articles) {
        const values = [
            article.title || null,   // Use null if missing
            article.content || null, // Use null if missing
            article.source.name || null, // Use null if missing
            article.publishedAt || null, // Use null if missing
            article.url || null, // Use null if missing
            article.urlToImage || null, // Use null if missing
        ];

        try {
            await client.query(insertQuery, values);
            console.log(`Inserted into temp_news: ${article.title}`);
        } catch (err) {
            console.error('Error inserting into temp_news', err);
        }
    }
}

async function deleteInvalidRows() {
    const deleteQuery = `
    DELETE FROM temp_news 
    WHERE title IS NULL OR content = '' OR content = '[Removed]'
       OR content IS NULL OR content = '' OR content = '[Removed]' 
       OR source_name IS NULL OR content = '' OR content = '[Removed]'
       OR published_at IS NULL OR content = '' OR content = '[Removed]'
       OR url IS NULL OR content = '' OR content = '[Removed]'
       OR img IS NULL OR content = '' OR content = '[Removed]';
    `;

    await client.query(deleteQuery);
    console.log('Deleted rows with missing or removed content from temp_news');
}

async function transferValidRowsToMainTable() {
    const insertQuery = `
    INSERT INTO news (title, content, source_name, published_at, url, img)
    SELECT title, content, source_name, published_at, url, img
    FROM temp_news;
    `;

    await client.query(insertQuery);
    console.log('Transferred valid rows to the main news table');
}

async function main() {
    await clearTable();    // Clear the existing news table
    await createTable();   // Create the table
    await createTempTable(); // Create the temporary table
    const articles = await fetchNews(); // Fetch the latest news
    await insertTempNews(articles); // Insert new articles into the temporary table
    await deleteInvalidRows(); // Delete rows with missing or removed content in the temporary table
    await transferValidRowsToMainTable(); // Transfer valid articles to the main table
    await client.end(); // Close the database connection
    console.log('Database connection closed');
}

// Execute the main function
main().catch(err => console.error(err));