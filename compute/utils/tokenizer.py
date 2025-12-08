# import nltk
# from nltk.stem import SnowballStemmer
# from nltk.corpus import stopwords
# import re
#
#
# class TextAnalyzer:
#     def __init__(self):
#         # 1. 自动下载 NLTK 数据 (幂等操作)
#         try:
#             nltk.data.find('tokenizers/punkt')
#             nltk.data.find('corpora/stopwords')
#         except LookupError:
#             print("⬇️ Downloading NLTK data...", flush=True)
#             nltk.download('punkt', quiet=True)
#             nltk.download('stopwords', quiet=True)
#             nltk.download('punkt_tab', quiet=True)
#
#         # 2. 初始化组件
#         self.stemmer = SnowballStemmer("english")
#         self.stop_words = set(stopwords.words('english'))
#
#     def analyze(self, text):
#         """
#         流程: 正则分词 -> 转小写 -> 去停用词 -> 词干提取
#         """
#         if not text: return []
#
#         # 使用正则分词，只保留由字母组成的单词 (长度>=2)
#         # 这比 nltk.word_tokenize 更快且更适合搜索引擎（忽略标点）
#         tokens = re.findall(r'\b[a-zA-Z]{2,}\b', text.lower())
#
#         clean_tokens = []
#         for token in tokens:
#             # 过滤停用词
#             if token in self.stop_words:
#                 continue
#
#             # 词干提取 (running -> run)
#             stemmed = self.stemmer.stem(token)
#             clean_tokens.append(stemmed)
#
#         return clean_tokens
#
#
# # 初始化单例
# analyzer = TextAnalyzer()


import re
import spacy
from spacy.cli import download as spacy_download


class TextAnalyzer:
    def __init__(self):
        # 1. 加载 spaCy 英文模型
        try:
            # 关闭不需要的组件，减少开销；保留 tagger 用于 lemmatization
            self.nlp = spacy.load(
                "en_core_web_sm",
                disable=["parser", "ner", "textcat"]
            )
        except OSError:
            # 如果模型没装，自动下载一次（如果你不想在代码里下载，可以注释掉）
            spacy_download("en_core_web_sm")
            self.nlp = spacy.load(
                "en_core_web_sm",
                disable=["parser", "ner", "textcat"]
            )

        # 2. 基础停用词表（spaCy 内置）
        base_stop = set(self.nlp.Defaults.stop_words)

        # 文档侧：可以使用完整停用词表
        self.doc_stop_words = base_stop

        # 查询侧：保留疑问词，避免把 who/what/why 这些删掉
        self.query_stop_words = base_stop - {
            "who", "what", "when", "where", "why", "how"
        }

        # 3. 额外允许的 token 形态
        # 比如 gpt-4o, http/1.1, ipv6, json_file 等
        self.special_token_pattern = re.compile(
            r"[a-z0-9]+(?:[-_.][a-z0-9]+)*$",
            re.IGNORECASE
        )

    def analyze(self, text: str, for_query: bool = False):
        """
        流程: spaCy 分词 -> 去停用词 -> 词形还原/规整
        返回: token 列表，用于索引或查询
        """
        if not text:
            return []

        doc = self.nlp(text)
        stop_words = self.query_stop_words if for_query else self.doc_stop_words

        tokens = []

        for token in doc:
            # 过滤空白和纯标点
            if token.is_space or token.is_punct:
                continue

            raw = token.text
            lower = raw.lower()

            # 先看停用词（统一按小写判断）
            if lower in stop_words:
                continue

            # 数字，例如 2024, 123, 3.14
            if token.like_num:
                tokens.append(lower)
                continue

            # 纯字母，使用 lemma（running -> run, better -> good 等）
            if token.is_alpha:
                lemma = token.lemma_.lower()
                if lemma and lemma not in stop_words:
                    tokens.append(lemma)
                continue

            # 其他混合形式，允许如 gpt-4o, http_1.1 之类的技术词
            if self.special_token_pattern.match(raw):
                tokens.append(lower)
                continue

            # 其它情况默认跳过（比如孤立的符号等）
            # 如果以后发现需要，可以再放宽规则

        return tokens


# 初始化单例
analyzer = TextAnalyzer()
