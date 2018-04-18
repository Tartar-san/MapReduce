from multiprocessing import Pool


class MapReducer:

    def __init__(self, number_of_mappers=4, number_of_reducers=4, use_combiner=False, number_of_combiners=4):
        self.number_of_mappers = number_of_mappers
        self.number_of_reducers = number_of_reducers
        self.use_combiner = use_combiner
        self.number_of_combiners = number_of_combiners

    def mapper(self, key, value):
        pass

    def reducer(self, key, values):
        pass

    def combiner(self, mapped_results):
        pass

    def run(self, input_data):
        mappers_pool = Pool(processes=self.number_of_mappers)
        reducers_pool = Pool(processes=self.number_of_reducers)
        if self.use_combiner:
            combiners_pool = Pool(processes=self.number_of_combiners)

        # map phase
        results = mappers_pool.starmap(self.mapper, input_data)

        # combine phase
        if self.use_combiner:
            results = combiners_pool.map(self.combiner, results)

        # grouping by key "shuffle" phase
        results = [x for l in results for x in l]
        grouped = {}

        for key, value in results:
            if key in grouped:
                grouped[key].append(value)
            else:
                grouped[key] = [value]

        grouped = [(key, grouped[key]) for key in grouped.keys()]

        # reduce phase
        reduced = reducers_pool.starmap(self.reducer, grouped)
        return reduced


if __name__ == "__main__":
    # create MapReduce object with map and reduce for word counting
    # MapReducer()
    pass

