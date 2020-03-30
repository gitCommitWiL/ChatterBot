import string
from chatterbot import languages
from chatterbot import singleton_classes
from nltk.corpus import stopwords
from spacy.lang.en.stop_words import STOP_WORDS
import re
stops = set(stopwords.words("english"))
newStops = STOP_WORDS.union(stops)
removeFromStop = {
    'us',
    'on',
    'off',
    'both',
    'give',
}
newStops = newStops - removeFromStop

class cleanTagger(object):
    def cleanUpText(self, text, removeStops=True):
        ## get rid of all special symbols
        cleanText = re.sub("([^A-Za-z0-9'\\s])", " ", text)
        words = []
        if removeStops:
            ## remove any stop words
            words = [w for w in cleanText.lower().split() if w not in newStops]
        ## if every word is removed, then go back to using original text but cleaned
        if not words:
            cleanText = cleanText.lower()
        else:
            cleanText = " ".join(words)
        return cleanText.strip()

class LowercaseTagger(object):
    """
    Returns the text in lowercase.
    """

    def __init__(self, language=None):
        self.language = language or languages.ENG

    def get_text_index_string(self, text):
        return text.lower()

class PosLemmaTagger(object):

    def __init__(self, language=None):

        self.language = language or languages.ENG

        self.punctuation_table = str.maketrans(dict.fromkeys(string.punctuation))

        self.nlp = singleton_classes.singleSpacy.getInstance(language)

    def get_text_index_string(self, text):
        """
        Return a string of text containing part-of-speech, lemma pairs.
        """

        def getBigrams(text):
            bigram_pairs = set()

            if len(text) <= 2:
                text_without_punctuation = text.translate(self.punctuation_table)
                if len(text_without_punctuation) >= 1:
                    text = text_without_punctuation

            document = self.nlp(text)

            if len(text) <= 2:
                bigram_pairs = {
                    token.lemma_.lower() for token in document
                }
            else:
                tokens = [
                    token for token in document if token.is_alpha# and not token.is_stop
                ]

                if len(tokens) < 2:
                    tokens = [
                        token for token in document if token.is_alpha
                    ]

                for index in range(1, len(tokens)):
                    bigram_pairs.add('{}:{}'.format(
                        tokens[index - 1].pos_,
                        tokens[index].lemma_.lower()
                    ))

            if not bigram_pairs:
                bigram_pairs = {
                    token.lemma_.lower() for token in document
                }

            return bigram_pairs
        bigRamSet = set()
        ## get bigrams for regular, and for cleaned without stopwords
        applyText = [text, cleanTagger().cleanUpText(text)]
        for t in applyText:
            bigRamSet = bigRamSet.union(getBigrams(t)) 
        return ' '.join(bigRamSet)
