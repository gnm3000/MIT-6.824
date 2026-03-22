import grpc
import os
from collections import defaultdict

import mapreduce_pb2
import mapreduce_pb2_grpc


WORKER_ADDR = "localhost:50051"


class Coordinator:

    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.channel = grpc.insecure_channel(WORKER_ADDR)
        self.stub = mapreduce_pb2_grpc.MapReduceServiceStub(self.channel)

    def load_inputs(self):
        inputs = []

        for f in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, f)

            with open(path, "r") as file:
                inputs.append(file.read())

        return inputs

    def run_map_phase(self, inputs):
        intermediate = []

        for text in inputs:
            # what MapRequest(data=text) does? 
            # It creates a MapRequest object with the data field set to the content of the file.
            # This object will be sent to the worker. 
            # The worker will run the map function on this data and return a 
            # list of key-value pairs. 
            # We will collect all the key-value pairs in a list called intermediate.
            
            
            
            
            
            # req = mapreduce_pb2.MapRequest(data=text) No envía nada. 
            # Solo instancia un mensaje protobuf en memoria.
            
            resp = self.stub.Map(
                        mapreduce_pb2.MapRequest(data=text)
                                 )
            # esto si envia el request al worker.
            # El worker ejecuta la función Map y devuelve un MapRepl
            # y que contiene una lista de pares clave-valor 
            # (key-value pairs) que representan el resultado del mapeo.
            
            # resp.pairs contains the list of key-value pairs returned by the worker.
            intermediate.extend(resp.pairs)

        return intermediate

    def shuffle(self, pairs):
        grouped = defaultdict(list)

        for kv in pairs:
            grouped[kv.key].append(kv.value)

        return grouped

    def run_reduce_phase(self, grouped):
        results = {}

        for key, values in grouped.items():

            req = mapreduce_pb2.ReduceRequest(
                key=key,
                values=values
            )

            resp = self.stub.Reduce(req)

            results[resp.key] = resp.value

        return results

    def run(self):

        print("Loading inputs")
        # this load all files. We have only one file, but in a real scenario we can have many files. We can also load data from a database or an API.
        inputs = self.load_inputs()

        print("Running MAP")
        # takes just one file content txt and sends it to the worker. 
        # The worker will run the map function and 
        # return a list of key-value pairs. 
        # We will collect all the key-value pairs 
        # from all the files and store them in a list called intermediate.
        pairs = self.run_map_phase(inputs)

        print("Shuffling")
        grouped = self.shuffle(pairs)

        print("Running REDUCE")
        results = self.run_reduce_phase(grouped)

        return results


if __name__ == "__main__":

    coord = Coordinator()
    result = coord.run()

    for k, v in sorted(result.items()):
        print(k, v)