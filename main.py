# source: https://medium.com/google-cloud/understanding-the-dataflow-quickstart-for-python-tutorial-e134f39564c7

import argparse
import apache_beam  as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

def run(argv=None):
    # Crates a new instance of the ArgumentParser class
    parser = argparse.ArgumentParser()

    # Adds an argument to listen for
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt')
    parser.add_argument(
        '--output',
        dest='output',
        required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Take the list of misc arguments and pass them as pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    
    with beam.Pipeline(options = pipeline_options) as p:
        output_pc = p | 'Read' >> ReadFromText(known_args.input)
        output_pc | 'Write' >> WriteToText(known_args.output)

    
if __name__ == '__main__':
    run()
