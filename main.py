import pandas as pd
import re
import time
import logging
from concurrent.futures import ProcessPoolExecutor

class ArticleReplacer:
    def __init__(self, articles_file: str, replacements_file: str, output_file: str):
        self.articles_file = articles_file
        self.replacements_file = replacements_file
        self.output_file = output_file

    def load_data(self):
        xls_articles = pd.ExcelFile(self.articles_file)
        xls_replacements = pd.ExcelFile(self.replacements_file)

        logging.info(f"Sheet names in articles file: {xls_articles.sheet_names}")
        logging.info(f"Sheet names in replacements file: {xls_replacements.sheet_names}")
        
        articles_df = pd.read_excel(self.articles_file, sheet_name=xls_articles.sheet_names[0], usecols=[0], names=["Article"], dtype=str)
        replacements_df = pd.read_excel(self.replacements_file, sheet_name=xls_replacements.sheet_names[0], dtype=str)

        return articles_df, replacements_df

    def prepare_replacements(self, replacements_df):
        replacements = []
        
        replace_from_columns = [col for col in replacements_df.columns if "Replace From" in col]
        replace_to_columns = [col for col in replacements_df.columns if "Replace To" in col]
        
        replace_pairs = list(zip(replace_from_columns, replace_to_columns))

        replacements_df = replacements_df.dropna(how="all", subset=replace_from_columns + replace_to_columns)


        for from_col, to_col in replace_pairs:
            for i, (from_value, to_value) in enumerate(zip(replacements_df[from_col], replacements_df[to_col])):
                if pd.notna(from_value) and pd.notna(to_value):  # Avoid NaN values
                    from_value = str(from_value).strip()
                    to_value = str(to_value).strip()  

                    if from_value and to_value:
                        replacements.append((to_value, from_value))  # Ensure they are strings
                else:
                    logging.debug(f"Skipping NaN value at row {i} ({from_value} -> {to_value})")  # Reduce logging noise

        return replacements
        
    def replace_text(self, text, replacements):
        for item in replacements:
            logging.debug(f"Processing replacement: {item} (type: {type(item)})")
            # if not isinstance(item, tuple) or len(item) != 2:
            if isinstance(item, tuple) and len(item) == 2 and all(isinstance(x, str) for x in item):
                logging.error(f"Invalid replacement format: {item} (skipping)")
                continue  # Skip invalid replacements

            old, new = item  # Unpack safely
            text = text.replace(old, new)
        return text


    def process_article(self, text, all_replacements):
        """Process the article text with all parts' replacements sequentially."""
        for replacements in all_replacements:
            text = self.replace_text(text, replacements)
        return text

    def process_articles(self, articles_df, all_replacements):
        start_time = time.time()
        with ProcessPoolExecutor() as executor:
            updated_articles = list(executor.map(self.process_article, articles_df["Article"], [all_replacements]*len(articles_df)))
        articles_df["Article"] = updated_articles
        logging.info(f"Replacement process completed in {time.time() - start_time:.2f} seconds")
        return articles_df

    def save_output(self, articles_df):
        articles_df.to_excel(self.output_file, index=False, sheet_name="Articles")
        logging.info(f"Output saved successfully as {self.output_file}")

    def run(self):
        articles_df, replacements_df = self.load_data()
        all_replacements = self.prepare_replacements(replacements_df)
        updated_articles_df = self.process_articles(articles_df, all_replacements)
        # self.save_output(updated_articles_df)
        print(type(all_replacements))
        print(all_replacements)


if __name__ == "__main__":
    replacer = ArticleReplacer(
        articles_file="Article.xlsx",
        replacements_file="Text To Replace.xlsx",
        output_file="output_articles.xlsx"
    )
    replacer.run()
