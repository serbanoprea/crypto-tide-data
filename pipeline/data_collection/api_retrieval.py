import luigi


class GetApiResult(luigi.Task):
    def run(self):
        raise NotImplementedError()
