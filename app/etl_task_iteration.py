from app.lib import pipe


class ETLTaskIteration:
    def __init__(self, source, transformer, sink):
        self.pipe_function = pipe(source.extract, transformer.transform, sink.load)

    def __call__(self):
        try:
            self.pipe_function()
        except Exception as e:
            print(f"Error during ETL iteration: {e}")
