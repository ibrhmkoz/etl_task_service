from app.kit.lib import pipe


class ETLTaskIteration:
    def __init__(self, source, transformer, sink):
        self.source = source
        self.sink = sink
        self.pipe_function = pipe(source.extract, transformer.transform, sink.load)

    def __call__(self):
        try:
            self.pipe_function()
        except Exception as e:
            self.clear_resources()
            print(f"Error during ETL iteration: {e}")

    def clear_resources(self):
        self.source.close()
        self.sink.close()
