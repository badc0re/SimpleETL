from transform.transformers import TransformerOne, TransformerTwo
from pipemodel.base import BaseRefinery
from extract.parser import ParseData
from load.to_file import toFile
import luigi



class ProviderRefinery(BaseRefinery):
    unique_identifier = luigi.Parameter('ProviderETL')
    output_folder = luigi.Parameter('./data/output_folder')
    input_file = './data/input_folder/data.json'

    _extract = [
        ParseData(input_file=input_file),
    ]

    _transform = [
        TransformerOne,
        TransformerTwo,
    ]

    _load = [
        toFile
    ]
