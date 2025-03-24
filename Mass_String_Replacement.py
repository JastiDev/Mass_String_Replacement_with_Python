import os
from datetime import datetime
import pandas as pd
import logging
import unicodedata
import re
from concurrent.futures import ProcessPoolExecutor
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

# Logging Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MAX_CELL_LENGTH = 32767  # Excel cell character limit


class FastReplacer:
    def __init__(self, articles_file, replacements_file):
        self.articles_file = articles_file
        self.replacements_file = replacements_file
        self.replacements = []  # Use list of tuples to preserve order
        self.load_replacements()

    def load_replacements(self):
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
                key, value = row[from_col], row[to_col]
                if key and value:
                    # Add replacements as tuple (key, value) to preserve order
                    self.replacements.append((re.escape(key), value))
        
        # Sort replacements by key length (longest first) to prevent substring issues
        self.replacements.sort(key=lambda x: len(x[0]), reverse=True)
        
        logging.info(f"✅ Loaded {len(self.replacements)} replacement pairs.")

    @staticmethod
    def sanitize_text(text):
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
        logging.info(f"🔄 Processing article {index + 1}/{total}...")

        if not isinstance(text, str) or not text.strip():
            return ""

        text = self.sanitize_text(text)
        
        for key, value in self.replacements:
            # Use whole word boundaries to prevent substring issues
            pattern = fr'\b{key}\b'  # Word boundaries to match whole words only
            text = re.sub(pattern, value, text, flags=re.IGNORECASE)
        
        return text

    def process_chunk(self, chunk, start_index, total):
        return [self.replace_text(text, start_index + i, total) for i, text in enumerate(chunk)]

    def split_long_text(self, text):
        return [text[i:i + MAX_CELL_LENGTH] for i in range(0, len(text), MAX_CELL_LENGTH)]

    def process_articles(self, progress_callback):
        logging.info("📥 Loading articles...")
        try:
            df = pd.read_excel(self.articles_file, sheet_name=0, dtype=str)
        except Exception as e:
            logging.error(f"❌ ERROR: Failed to read articles file {self.articles_file} - {e}")
            return

        if df.empty or df.shape[1] == 0:
            logging.error("❌ ERROR: No data found in the articles file!")
            return

        column_name = df.columns[0]
        articles = df[column_name].dropna().astype(str).tolist()
        logging.info(f"✅ Loaded {len(articles)} articles.")

        chunk_size = max(1000, len(articles) // 4)

        with ProcessPoolExecutor() as executor:
            result_chunks = list(executor.map(self.process_chunk, 
                                              [articles[i:i + chunk_size] for i in range(0, len(articles), chunk_size)],
                                              range(0, len(articles), chunk_size),
                                              [len(articles)] * (len(articles) // chunk_size + 1)))

        updated_articles = [text for chunk in result_chunks for text in chunk]
        progress_callback(70)
        
        split_data = [self.split_long_text(article) for article in updated_articles]
        max_columns = max(len(parts) for parts in split_data)
        column_names = [f"Article Part {i + 1}" for i in range(max_columns)]
        output_df = pd.DataFrame(split_data, columns=column_names)

        today_date = datetime.today().strftime("%Y%m%d")

        logging.info("💾 Saving updated articles...")
        output_df.to_excel(f"output_articles-{today_date}.xlsx", index=False)
        logging.info("✅ Processing complete! Output saved 🎉")
        progress_callback(100)


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("FastReplacer - Enhanced UI")
        self.geometry("500x300")
        self.configure(bg="#f0f0f0")
        
        ttk.Label(self, text="Select Article File:").pack(pady=5)
        self.article_entry = ttk.Entry(self, width=60)
        self.article_entry.pack(pady=5)
        ttk.Button(self, text="Browse", command=lambda: self.browse_file(self.article_entry)).pack()
        
        ttk.Label(self, text="Select Replace Text File:").pack(pady=5)
        self.replace_entry = ttk.Entry(self, width=60)
        self.replace_entry.pack(pady=5)
        ttk.Button(self, text="Browse", command=lambda: self.browse_file(self.replace_entry)).pack()
        
        self.progress = ttk.Progressbar(self, orient="horizontal", length=400, mode="determinate")
        self.progress.pack(pady=10)
        
        ttk.Button(self, text="Start Process", command=self.start_replacer).pack(pady=10)
        
    def browse_file(self, entry):
        filename = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx"), ("All Files", "*.*")])
        if filename:
            entry.delete(0, tk.END)
            entry.insert(0, filename)
        
    def start_replacer(self):
        path1 = self.article_entry.get()
        path2 = self.replace_entry.get()
        
        if not os.path.exists(path1) or not os.path.exists(path2):
            messagebox.showerror("Error", "Selected files do not exist!")
            return
        
        replacer = FastReplacer(path1, path2)
        replacer.process_articles(self.update_progress)
        messagebox.showinfo("Success", "Processing complete!")
        
    def update_progress(self, value):
        self.progress["value"] = value
        self.update_idletasks()


if __name__ == "__main__":
    app = App()
    app.mainloop()
