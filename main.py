# source: https://medium.com/google-cloud/understanding-the-dataflow-quickstart-for-python-tutorial-e134f39564c7

import logging
import argparse
import re

import apache_beam  as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""
    def process(self, element):
        """ Returns an iterator over the words of this element.
            The element is a lineo f text. If the line is blank, note that, too
            Args:
                element: the element being processed
            Returns:
                The processed element.
        """
        print(element)
        words = re.findall(r'[\w\']+', element, re.UNICODE)
        print(words)
        input()

        return words
    

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
        output_pc = output_pc | "Split" >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
        output_pc = output_pc | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        output_pc | 'Write' >> WriteToText(known_args.output)

    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
