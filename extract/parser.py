from pipemodel.base import BaseTask
import luigi
import json


class Loader(luigi.Task):
    pass


class FileLoader(Loader):
    input_file = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.input_file)


# Sample task
class ParseData(BaseTask):
    input_file = luigi.Parameter()

    def requires(self):
        return FileLoader(self.input_file)

    def run(self):
        input_data = json.loads(self.from_input())
        input_data.append({self.__class__.__name__: self.__class__.__name__})
        self.to_output(input_data)
