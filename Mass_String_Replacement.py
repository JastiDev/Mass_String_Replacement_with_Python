import pandas as pd
import logging
import unicodedata
from concurrent.futures import ProcessPoolExecutor

# Logging Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MAX_CELL_LENGTH = 32767  # Excel cell character limit

class FastReplacer:
    def __init__(self, articles_file, replacements_file):
        self.articles_file = articles_file
        self.replacements_file = replacements_file
        self.replacements = {}
        self.load_replacements()

    def load_replacements(self):
        """Load replacement words from all 'Replace From' → 'Replace To' pairs in multiple parts."""
        logging.info("🔄 Loading replacements...")
        try:
            df = pd.read_excel(self.replacements_file, sheet_name=0, dtype=str)
        except Exception as e:
            logging.error(f"❌ ERROR: Failed to read replacement file {self.replacements_file} - {e}")
            return

        replace_from_cols = [col for col in df.columns if "Replace From" in col]
        replace_to_cols = [col for col in df.columns if "Replace To" in col]

        if not replace_from_cols or not replace_to_cols:
            logging.error("❌ ERROR: No 'Replace From' and 'Replace To' columns detected in Excel!")
            return

        for from_col, to_col in zip(replace_from_cols, replace_to_cols):
            df_filtered = df[[from_col, to_col]].dropna()
            for _, row in df_filtered.iterrows():
                original_key, original_value = row[from_col], row[to_col]
                key, value = original_key.strip(), original_value.strip()

                # Debugging: Check if spaces were present before trimming
                if original_key != key or original_value != value:
                    logging.info(f"✅ Trimming spaces: '{original_key}' → '{key}' | '{original_value}' → '{value}'")

                if key and value:
                    self.replacements[key] = value

        logging.info(f"✅ Loaded {len(self.replacements)} replacement words from {len(replace_from_cols)} parts.")

    @staticmethod
    def sanitize_text(text):
        """Normalize text and remove control characters."""
        if not isinstance(text, str) or not text.strip():
            return ""
        try:
            text = unicodedata.normalize("NFKC", text)
            text = ''.join(c for c in text if c.isprintable())
            text = text.replace("\u200b", "").replace("\ufeff", "").strip()
            return text
        except Exception as e:
            logging.error(f"⚠️ Error sanitizing text: {repr(text)[:50]}... - {e}")
            return text

    def replace_text(self, text, index, total):
        """Perform text replacement using the preloaded dictionary."""
        logging.info(f"🔄 Processing article {index+1}/{total}...")

        if not isinstance(text, str) or not text.strip():
            return ""

        text = self.sanitize_text(text)

        for key, value in self.replacements.items():
            if key in text:
                text = text.replace(key, value)
        
        return text

    def process_chunk(self, chunk, start_index, total):
        """Process a chunk of articles."""
        return [self.replace_text(text, start_index + i, total) for i, text in enumerate(chunk)]

    def split_long_text(self, text):
        """Split text into multiple parts if it exceeds Excel's cell limit."""
        return [text[i:i + MAX_CELL_LENGTH] for i in range(0, len(text), MAX_CELL_LENGTH)]

    def process_articles(self):
        """Load, process, and save updated articles."""
        logging.info("📥 Loading articles...")
        try:
            df = pd.read_excel(self.articles_file, sheet_name=0, dtype=str)
        except Exception as e:
            logging.error(f"❌ ERROR: Failed to read articles file {self.articles_file} - {e}")
            return

        if df.empty or df.shape[1] == 0:
            logging.error("❌ ERROR: No data found in the articles file!")
            return

        column_name = df.columns[0]  # Assume first column contains text
        articles = df[column_name].dropna().astype(str).tolist()
        logging.info(f"✅ Loaded {len(articles)} articles.")

        chunk_size = max(1000, len(articles) // 4)

        with ProcessPoolExecutor() as executor:
            result_chunks = list(executor.map(self.process_chunk, 
                                              [articles[i:i + chunk_size] for i in range(0, len(articles), chunk_size)],
                                              range(0, len(articles), chunk_size),
                                              [len(articles)] * (len(articles) // chunk_size + 1)))

        updated_articles = [text for chunk in result_chunks for text in chunk]

        logging.info("💾 Splitting long articles...")
        split_data = []
        for article in updated_articles:
            split_data.append(self.split_long_text(article))

        max_columns = max(len(parts) for parts in split_data)
        column_names = [f"Article Part {i+1}" for i in range(max_columns)]
        output_df = pd.DataFrame(split_data, columns=column_names)

        logging.info("💾 Saving updated articles...")
        output_df.to_excel("output_articles.xlsx", index=False)
        logging.info("✅ Processing complete! Output saved as output_articles.xlsx 🎉")


if __name__ == "__main__":
    replacer = FastReplacer("Article.xlsx", "Text To Replace.xlsx")
    replacer.process_articles()
