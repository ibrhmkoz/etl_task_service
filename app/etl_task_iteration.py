from app.kit.lib import pipe


class ETLTaskIteration:
    def __init__(self, source, transformer, sink):
        self.source = source
        self.sink = sink
        self.pipe_function = pipe(source.extract, transformer.transform, sink.load)

    def __call__(self):
        self.pipe_function()
