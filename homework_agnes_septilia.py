"""
Download Script Film The Abyss https://www.dailyscript.com/scripts/abyss.html dan ubah menjadi format txt.

Step Homework:
Map semua kata (alphanumeric) yang ada di Script tersebut dengan standard uppercase. 
Hitung total dari setiap kata yang muncul di Script tersebut. 
Cari kata yang paling banyak disebutkan di Script tersebut.
buat counter di proses mapper dan reducer
"""

# import libraries 
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# variable to search only alphanumeric 
WORD_RE = re.compile(r"[A-Za-z0-9]+")


class MRScreening(MRJob):

    def mapper_get_words(self, _, line):
        # yield each word
        for word in WORD_RE.findall(line):
            self.increment_counter('group', 'lines', 1)
            yield (word.upper(), 1)

    def combiner_count_words(self, word, counts):
        # sum the words we've seen so far
        self.increment_counter('group', 'combiner', 1)
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        self.increment_counter('group', 'reducers', 1)
        yield None, (sum(counts), word)

    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        self.increment_counter('group', 'reducers_2', 1)
        yield max(word_count_pairs)
        
    def steps(self):
        # define the step of reducer
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

# run the mrjob
if __name__ == '__main__':
    MRScreening.run()