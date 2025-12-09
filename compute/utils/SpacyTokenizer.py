# Deprecated: give up on semantic search for simplicity.
# Not much difference  with or without practice
# Might Because BM25 & PageRank already cut down a lot of candidates which can be semantically relevant
# Saved for possible future use
# Guess:
# Try to give more candidates in semantic ranking stage?
# maybe BM25 weight too much?

# import re
# import spacy
# from spacy.cli import download as spacy_download
#
#
# class SpacyTextAnalyzer:
#     def __init__(self):
#         try:
#
#             self.nlp = spacy.load(
#                 "en_core_web_sm",
#                 disable=["parser", "ner", "textcat"]
#             )
#         except OSError:
#             spacy_download("en_core_web_sm")
#             self.nlp = spacy.load(
#                 "en_core_web_sm",
#                 disable=["parser", "ner", "textcat"]
#             )
#
#         base_stop = set(self.nlp.Defaults.stop_words)
#
#         self.doc_stop_words = base_stop
#
#         # Try to keep question words for queries
#
#         self.query_stop_words = base_stop - {
#             "who", "what", "when", "where", "why", "how"
#         }
#
#
#         self.special_token_pattern = re.compile(
#             r"[a-z0-9]+(?:[-_.][a-z0-9]+)*$",
#             re.IGNORECASE
#         )
#
#     def analyze(self, text: str, for_query: bool = False):

#         if not text:
#             return []
#
#         doc = self.nlp(text)
#         stop_words = self.query_stop_words if for_query else self.doc_stop_words
#
#         tokens = []
#
#         for token in doc:
#             if token.is_space or token.is_punct:
#                 continue
#
#             raw = token.text
#             lower = raw.lower()
#
#             if lower in stop_words:
#                 continue
#
#             if token.like_num:
#                 tokens.append(lower)
#                 continue
#
#             if token.is_alpha:
#                 lemma = token.lemma_.lower()
#                 if lemma and lemma not in stop_words:
#                     tokens.append(lemma)
#                 continue
#
#             if self.special_token_pattern.match(raw):
#                 tokens.append(lower)
#                 continue
#
#
#
#         return tokens
#
#
# analyzer = TextAnalyzer()
