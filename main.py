# source: https://medium.com/google-cloud/understanding-the-dataflow-quickstart-for-python-tutorial-e134f39564c7

import logging
import argparse
import re

import apache_beam  as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

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
    

def run(argv=None, save_main_session=True):
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
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    with beam.Pipeline(options = pipeline_options) as p:
        lines = p | "Read" >> ReadFromText(known_args.input)

        counts = (
            lines
            | "Split" >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
            | "LowerChars" >> beam.Map(lambda w: w.lower())
            | "PairWithOne" >> beam.Map(lambda x: (x, 1))
            | "GroupAndSum" >> beam.CombinePerKey(sum)
        )

        def format_result(word, count):
            return "%s: %d" % (word, count)
        
        output = counts | "Format" >> beam.MapTuple(format_result)
        output | 'Write' >> WriteToText(known_args.output)

    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
