import nltk
from nltk.stem import SnowballStemmer
from nltk.corpus import stopwords
import re


class TextAnalyzer:
    def __init__(self):
        # 1. 自动下载 NLTK 数据 (幂等操作)
        try:
            nltk.data.find('tokenizers/punkt')
            nltk.data.find('corpora/stopwords')
        except LookupError:
            print("⬇️ Downloading NLTK data...", flush=True)
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
            nltk.download('punkt_tab', quiet=True)

        # 2. 初始化组件
        self.stemmer = SnowballStemmer("english")
        self.stop_words = set(stopwords.words('english'))

    def analyze(self, text):
        """
        流程: 正则分词 -> 转小写 -> 去停用词 -> 词干提取
        """
        if not text: return []

        # 使用正则分词，只保留由字母组成的单词 (长度>=2)
        # 这比 nltk.word_tokenize 更快且更适合搜索引擎（忽略标点）
        tokens = re.findall(r'\b[a-zA-Z]{2,}\b', text.lower())

        clean_tokens = []
        for token in tokens:
            # 过滤停用词
            if token in self.stop_words:
                continue

            # 词干提取 (running -> run)
            stemmed = self.stemmer.stem(token)
            clean_tokens.append(stemmed)

        return clean_tokens


# 初始化单例
analyzer = TextAnalyzer()