from pipemodel.base import BaseTask
import json


class toFile(BaseTask):
    def run(self):
        input_data = json.loads(self.from_input())
        input_data.append({self.__class__.__name__: self.__class__.__name__})
        self.to_output(input_data)

