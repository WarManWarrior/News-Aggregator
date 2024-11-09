import os
os.environ['CUDA_LAUNCH_BLOCKING'] = '1'
import torch

from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
import torch.nn.functional as F
import psycopg2
from tqdm import tqdm
import gc
import logging
from typing import List, Tuple, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TextProcessor:
    def __init__(self):
        # Check if GPU is available and has enough memory
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        if self.device == "cuda":
            # Set to a lower number if experiencing memory issues
            torch.cuda.set_per_process_memory_fraction(0.7)
        
        logger.info(f"Using device: {self.device}")
        
        # Initialize models with proper error handling
        try:
            # Initialize pipelines with smaller batch sizes
            self.sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model="siebert/sentiment-roberta-large-english",
                device=self.device,
                batch_size=1
            )
            
            self.summarizer = pipeline(
                "summarization",
                model="facebook/bart-large-cnn",
                device=self.device,
                batch_size=1
            )
            
            # Initialize sentiment model
            model_name = "cardiffnlp/twitter-roberta-base-sentiment"
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name).to(self.device)
            
        except Exception as e:
            logger.error(f"Error initializing models: {str(e)}")
            raise

    def analyze_sentiment(self, text: str) -> Optional[str]:
        """Analyze sentiment of the given text with error handling."""
        try:
            # Clear CUDA cache before processing
            if self.device == "cuda":
                torch.cuda.empty_cache()
                
            inputs = self.tokenizer(text, return_tensors='pt', truncation=True, max_length=512).to(self.device)
            with torch.no_grad():
                output = self.model(**inputs)
            probs = F.softmax(output.logits, dim=1)
            sentiment = torch.argmax(probs, dim=-1).item()
            sentiment_map = {0: "negative", 1: "neutral", 2: "positive"}
            return sentiment_map[sentiment]
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {str(e)}")
            return None
        finally:
            # Clear memory
            if self.device == "cuda":
                torch.cuda.empty_cache()
            gc.collect()

    def summarize_text(self, text: str) -> Optional[str]:
        """Summarize the given text with error handling."""
        try:
            # Clear CUDA cache before processing
            if self.device == "cuda":
                torch.cuda.empty_cache()
                
            summary = self.summarizer(
                text,
                max_length=130,
                min_length=30,
                do_sample=False,
                truncation=True
            )[0]['summary_text']
            return summary
        except Exception as e:
            logger.error(f"Error in text summarization: {str(e)}")
            return None
        finally:
            # Clear memory
            if self.device == "cuda":
                torch.cuda.empty_cache()
            gc.collect()

class DatabaseHandler:
    def __init__(self, host: str, database: str, user: str, password: str):
        self.connection_params = {
            "host": host,
            "database": database,
            "user": user,
            "password": password
        }

    def __enter__(self):
        self.conn = psycopg2.connect(**self.connection_params)
        self.cur = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def setup_table(self):
        """Set up the news_articles table."""
        self.cur.execute("DROP TABLE IF EXISTS news_articles")
        self.cur.execute("""
            CREATE TABLE news_articles (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                content TEXT
            )
        """)
        self.conn.commit()

    def fetch_data(self) -> List[str]:
        """Fetch data from sample table."""
        self.cur.execute("SELECT content FROM sample")
        return [row[0] for row in self.cur.fetchall()]

    def insert_results(self, sentiments: List[str], summaries: List[str]):
        """Insert results into the database."""
        for sentiment, summary in zip(sentiments, summaries):
            if sentiment and summary:
                self.cur.execute(
                    "INSERT INTO news_articles (title, content) VALUES (%s, %s)",
                    (sentiment, summary)
                )
        self.conn.commit()

def main():
    # Database connection details
    db_params = {
        "host": "localhost",
        "database": "Aggre",
        "user": "postgres",
        "password": "siddh2003"
    }

    try:
        # Initialize text processor
        processor = TextProcessor()
        
        # Process data in batches
        with DatabaseHandler(**db_params) as db:
            logger.info("Setting up database table...")
            db.setup_table()
            
            logger.info("Fetching data...")
            text_data = db.fetch_data()
            
            sentiments = []
            summaries = []
            
            logger.info("Processing texts...")
            for text in tqdm(text_data):
                if not text or not isinstance(text, str):
                    sentiments.append(None)
                    summaries.append(None)
                    continue
                    
                try:
                    summary = processor.summarize_text(text)
                    sentiment = processor.analyze_sentiment(text) if summary else None
                    summaries.append(summary)
                    sentiments.append(sentiment)
                except Exception as e:
                    logger.error(f"Error processing text: {str(e)}")
                    summaries.append(None)
                    sentiments.append(None)
            
            logger.info("Inserting results into database...")
            db.insert_results(sentiments, summaries)
            
        logger.info("Processing completed successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise


def test_run(text):
    processor = TextProcessor()
    summary = processor.summarize_text(text)
    sentiment = processor.analyze_sentiment(summary)
    return summary, sentiment

if __name__ == "__main__":
    s = "Copyright 2024 The Associated Press. All Rights Reserved. A comic calling Puerto Rico garbage before a packed Donald Trump rally in New York was the latest humiliation for an island territory that has long suffered from mistreatment, residents said Monday in expressions of fury that could affect the presidential election. (AP Video by Alejandro Granadillo) Trump dismissed claims that he or his supporters were comparable to Nazis in his rally at Georgia Tech in midtown Atlanta. (AP video by Erik Verduzco) Republican presidential nominee former President Donald Trump speaks during a campaign rally at McCamish Pavilion Monday, Oct. 28, 2024, in Atlanta, Ga. (AP Photo/Julia Demaree Nikhinson) Tony Hinchcliffe arrives to speak before Republican presidential nominee former President Donald Trump during a campaign rally at Madison Square Garden, Sunday, Oct. 27, 2024, in New York. (AP Photo/Evan Vucci) Democratic presidential nominee Vice President Kamala Harris speaks during a campaign rally at Burns Park in Ann Arbor, Mich., Monday, Oct. 28, 2024. (AP Photo/Carlos Osorio) Republican presidential nominee former President Donald Trump arrives at a campaign rally at McCamish Pavilion Monday, Oct. 28, 2024, in Atlanta, Ga. (AP Photo/Julia Demaree Nikhinson) Republican presidential nominee former President Donald Trump arrives for the National Faith Summit at Worship With Wonders Church, Monday, Oct. 28, 2024, in Powder Springs, Ga. (AP Photo/Julia Demaree Nikhinson) Tony Hinchcliffe speaks before Republican presidential nominee former President Donald Trump during a campaign rally at Madison Square Garden, Sunday, Oct. 27, 2024, in New York. (AP Photo/Evan Vucci) Tony Hinchcliffe arrives to speak before Republican presidential nominee former President Donald Trump during a campaign rally at Madison Square Garden, Sunday, Oct. 27, 2024, in New York. (AP Photo/Evan Vucci) Democratic presidential nominee Vice President Kamala Harris, from second right, tours the Hemlock Semiconductor Next-Generation Finishing facility as Corning Chairman and CEO Wendell Weeks looks on, in Hemlock, Mich., Monday, Oct. 28, 2024. (AP Photo/Jacquelyn Martin) Democratic presidential nominee Vice President Kamala Harris speaks after taking a tour of the Hemlock Semiconductor Next-Generation Finishing facility in Hemlock, Mich., Monday, Oct. 28, 2024. (AP Photo/Jacquelyn Martin) Democratic presidential nominee Vice President Kamala Harris greets employees after speaking at the Hemlock Semiconductor Next-Generation Finishing facility in Hemlock, Mich., Monday, Oct. 28, 2024. (AP Photo/Jacquelyn Martin) Republican presidential nominee former President Donald Trump arrives for the National Faith Summit at Worship With Wonders Church, Monday, Oct. 28, 2024, in Powder Springs, Ga. (AP Photo/Julia Demaree Nikhinson) Republican presidential nominee former President Donald Trump arrives at a campaign rally at McCamish Pavilion Monday, Oct. 28, 2024, in Atlanta. (AP Photo/Mike Stewart) Democratic presidential nominee Vice President Kamala Harris speaks at a campaign event in Burns Park Monday, Oct. 28, 2024, in Ann Arbor, Mich. (AP Photo/Paul Sancya) Follow live:Updates from AP’s coverage of thepresidential election. WASHINGTON (AP) — Democrats stepped up their attacks on Donald Trump on Monday, a day after a comedian opening a rally for the former president called Puerto Rico a “floating island of garbage,” a comment that drew wide condemnation and highlighted the rising power ofa key Latino group in the swing state of Pennsylvania. Vice President Kamala Harris described Trump’s rally Sunday atMadison Square Gardenas “more vivid than usual” and said he “fans the fuel of hate” before she flew to Michigan for a campaign event. President Joe Biden called the rally “simply embarrassing.” In a rare move late Sunday, the Trump campaign distanced itself from the remarks on Puerto Rico made by comedian Tony Hinchcliffe. “The garbage he spoke about is polluting our elections and confirming just how little Donald Trump cares about Latinos specifically, about our Puerto Rican community,” Eddie Moran, mayor of Reading, said at a news conference with other Puerto Rican officials. With just over a week before Election Day, the fallout underscores the importance of Pennsylvania’s 19 electoral votes and the last-minute efforts to court growing numbers of Hispanic voters, mostly from Puerto Rico, who have settled in cities west and north of Philadelphia. Fernando Tormos-Aponte, an assistant professor of sociology at the University of Pittsburgh who specializes in Puerto Rican politics and electoral organizing, said the timing of the comments may spell trouble for the Trump campaign. “When you combine the events that took place yesterday with other grievances that Puerto Ricans have, you really are not engaging in sound political strategy,” Tormos-Aponte said. Trump did not directly mention the controversy during his appearances in Georgia Monday, instead choosing to parry another critique of him — that his former White House chief of staff reports that Trump as president said he wished he had “German generals.” The Harris campaign has seized on the comment and the vice president, in a radio interview last week, agreed that Trump was “a fascist.” During a Monday night rally at Georgia Tech in Atlanta, Trump instead called Harris a “fascist” and said: “I’m not a Nazi. I’m the opposite of a Nazi.” Trump also warned that Michelle Obama made a “big mistake” by being “nasty” to him in a recent speech. During his first appearance of the day, a National Faith Summit in Powder Springs, Georgia, conservative activist Gary Bauer asked a question that included offhand praise for Trump turning Madison Square Garden “into MAGA Square Garden.” “Great night,” Trump replied. Trump’s vice presidential pick, Sen. JD Vance, was asked about the insult during an appearance in Wausau, Wisconsin. “Maybe it’s a stupid racist joke, as you said. Maybe it’s not. I haven’t seen it. I’m not going to comment on the specifics of the joke,” Vance said. “But I think that we have to stop getting so offended at every little thing.” The Harris campaign released an ad that will run online in battleground states targeting Puerto Rican voters and highlighting the comedian’s remarks. The comments landed Harrisa show of support from Puerto Rican music star Bad Bunnyand prompted reactions from Republicans in Florida and in Puerto Rico. Hinchcliffe also made demeaning jokes about Black people, other Latinos, Palestinians and Jews in his routine before Trump’s appearance. On Monday in Pittsburgh, Harris’ husband, Doug Emhoff, who is Jewish, delivered remarks on antisemitism in America, a day after the anniversary of the Tree of Life synagogue massacre. What to know about the 2024 Election “There is a fire in this country, and we either pour water on it or we pour gasoline on it,” Emhoff said. Still, it was Hinchliffe’s quip about Puerto Rico that drew the most attention, partly due to the geography of the election. From Labor Day to this past weekend, both campaigns have made more visits to Pennsylvania than to Georgia, Arizona and Nevada combined, according to Associated Press tracking of the campaigns’ public events. The state has some of the fastest-growing Hispanic communities, including in Reading and Allentown, where more than half of the population is Hispanic. Pennsylvania’s Latino eligible voter population has more than doubled since 2000, from 206,000 to 620,000 in 2023, according to Census Bureau figures. More than half of those are Puerto Rican eligible voters. The comedian’s remarks were played early Monday on Spanish-language radio in Pennsylvania by one of Harris’ surrogates based in Allentown, Pennsylvania, who called out Trump for not issuing an apology beyond a statement from the campaign saying “this joke does not reflect the views of President Trump or the campaign.” In central Florida, U.S. Rep. Darren Soto, a Democrat whose district covers neighborhoods with large numbers of Puerto Ricans recently moved from the island, noted Monday that there are “huge numbers” of Puerto Ricans in swing states. “We remember, and you know what, we are going to vote,” Soto said at a news conference called by Puerto Rican leaders. “That’s the only thing we can do right now.” Harris said Monday that none of the vitriol at the Madison Square Garden rally will support the dreams and aspirations of the American people but instead fans “the fuel of trying to divide our country.” She said Trump’s event Sunday, in which speakers hurled cruel and racist insults, “highlighted the point that I’ve been making throughout this campaign.” “He is focused and actually fixated on his grievances, on himself and on dividing our country, and it is not in any way something that will strengthen the American family, the American worker,” the Democratic presidential nominee told reporters. Harris also said: “What he did last night is not a discovery. It is just more of the same and may be more vivid than usual. Donald Trump spends full time trying to have Americans point their finger at each other, fans the fuel of hate and division, and that’s why people are exhausted with him.” Harris also spoke about her proposals for Puerto Rico, such as creating a task force to bring in private companies to upgrade the island’s electrical grid. Trump planned to return to Pennsylvania on Tuesday with a visit to Allentown after delivering remarks to reporters at his Mar-a-Lago resort in South Florida. Gomez Licon reported from Fort Lauderdale, Florida, and Price from Atlanta. Associated Press writers Bill Barrow in Atlanta, Nicholas Riccardi in Denver, Mike Schneider in Orlando and Will Weissert in Washington contributed to this report. Copyright 2024 The Associated Press. All Rights Reserved."
    print(test_run(s))