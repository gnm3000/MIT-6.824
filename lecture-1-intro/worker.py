import grpc
from concurrent import futures
import time
import mapreduce_pb2
import mapreduce_pb2_grpc


class Worker(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def Map(self, request, context):
        text = request.data

        words = text.split()
        result = {}

        for w in words:
            result[w] = result.get(w, 0) + 1

        kvs = []
        # what mapreduce_pb2.KeyValue does and where exec?
        # It creates a KeyValue object with the key and value fields 
        # set to the word and its count, respectively.
        # This object will be sent back to the coordinator as part of the MapReply.
        for k, v in result.items():
            kvs.append(
                mapreduce_pb2.KeyValue(key=k, value=str(v))
            )

        return mapreduce_pb2.MapReply(pairs=kvs)

    def Reduce(self, request, context):
        key = request.key
        try:
            values = [int(v) for v in request.values]
        except ValueError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Reduce values must be integers encoded as strings")
            return mapreduce_pb2.ReduceReply()

        total = sum(values)

        return mapreduce_pb2.ReduceReply(
            key=key,
            value=str(total)
        )


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(
        Worker(),
        server
    )

    server.add_insecure_port("[::]:50051")
    server.start()

    print("Worker running on port 50051")

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
