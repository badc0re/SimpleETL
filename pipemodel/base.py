import inspect
import luigi
import json
import sys
import os


class BaseRefinery(luigi.WrapperTask):
    def requires(self):
        tasks = self._extract + self._transform + self._load
        temp_task = None
        for task in tasks:
            if inspect.isclass(task):
                if temp_task:
                    task.dependency_task = temp_task
                # note the extra fields will be remove
                # after the first PARSE class
                for field in self.param_kwargs.keys():
                    if field not in BaseTask._fields:
                        del self.param_kwargs[field]
                temp_task = task(**self.param_kwargs)
            else:
                for field in task.param_kwargs.keys():
                    if field in self.param_kwargs and \
                            self.param_kwargs[field] not in [None, '__null']:
                        task.param_kwargs[field] = self.param_kwargs[field]
                temp_task = type(task)(**task.param_kwargs)
            yield temp_task


class BaseTask(luigi.Task):
    unique_identifier = luigi.Parameter(default='__null')
    output_folder = luigi.Parameter(default='__null')
    # This is funky
    _fields = ['unique_identifier', 'output_folder']
    dependency_task = None

    def __init__(self, *args, **kwargs):
        super(BaseTask, self).__init__(*args, **kwargs)

    def requires(self):
        if self.dependency_task:
            return self.dependency_task

    def from_input(self):
        with self.input().open('r') as input_file:
            return input_file.read()

    def output(self):
        output_location = os.path.join(self.output_folder,
                                       self.unique_identifier,
                                       self.__class__.__name__ + ".json")
        return luigi.LocalTarget(output_location)

    def to_output(self, input_data):
        if isinstance(input_data, str):
            raise ValueError("Dumping to output file must be a dict!")

        with self.output().open('w') as output_file:
            output_file.write(json.dumps(input_data))

