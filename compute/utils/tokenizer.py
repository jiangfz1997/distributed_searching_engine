# Using NLTK for tokenization, stemming, and stopword removal
# Stopping words can be optimized maybe?
# Myabe there's better way for tokenization than regex, current solution could infect semantics


import nltk
from nltk.stem import SnowballStemmer
from nltk.corpus import stopwords
import re

class TextAnalyzer:
    def __init__(self):
        try:
            nltk.data.find('tokenizers/punkt')
            nltk.data.find('corpora/stopwords')
        except LookupError:
            print("Downloading NLTK data...", flush=True)
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
            nltk.download('punkt_tab', quiet=True)

        self.stemmer = SnowballStemmer("english")
        self.stop_words = set(stopwords.words('english'))

    def analyze(self, text):
        if not text: return []

        # Tokenization using regex to capture words and numbers
        # keep the numbers (can find iphone 14 etc.)
        # but numbers might have really high bm25 score?
        tokens = re.findall(r'\b[a-zA-Z0-9]{1,}\b', text.lower())

        clean_tokens = []
        for token in tokens:
            if token in self.stop_words:
                continue

            if token.isdigit():
                clean_tokens.append(token)
                continue

            stemmed = self.stemmer.stem(token)
            clean_tokens.append(stemmed)
        return clean_tokens

analyzer = TextAnalyzer()
