from MapReducer import MapReducer
import re
import os


class DataReader:

    def __init__(self, input_directory):
        self.data = []
        self.read_all(input_directory)

    def read_csv(self, file):
        for line in file:
            self.data.append(line.replace(",", " ").rstrip())

    def read_txt(self, file):
        for line in file:
            self.data.append(line.rstrip())

    def read_all(self, path):

        for filename in os.listdir(path):
            with open(os.path.join(path, filename)) as file:
                if re.match(r'.*?\.csv$',filename):
                    self.read_csv(file)
                else:
                    self.read_txt(file)

    def get_chunks(self, number_of_chunks):
        size_of_chunk = int(len(self.data)/number_of_chunks)
        chunks = [self.data[i*size_of_chunk: (i+1)*size_of_chunk] for i in range(number_of_chunks)]
        chunks = [" ".join(chunk) for chunk in chunks]
        chunks = [(i, chunk) for i,chunk in enumerate(chunks)]
        return chunks


class WordCounter(MapReducer):

    def mapper(self, key, value):
        results = []

        for word in value.split():
            results.append((word.lower(), 1))
        return results

    def combiner(self, mapped_results):
        number_of_words = {}

        for key, value in mapped_results:
            if key in number_of_words:
                number_of_words[key] += value
            else:
                number_of_words[key] = value

        return [(key, number_of_words[key]) for key in number_of_words.keys()]

    def reducer(self, key, values):
        number_of_words = sum(values)
        return key, number_of_words


if __name__ == "__main__":
    reader = DataReader("input_files")
    input_data = reader.get_chunks(8)
    # print(input_data)
    # word_counter = WordCounter()
    # print(word_counter.run(input_data))

    word_counter_with_combiners = WordCounter(use_combiner=True)
    print(word_counter_with_combiners.run(input_data))


