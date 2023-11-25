# source: https://medium.com/google-cloud/understanding-the-dataflow-quickstart-for-python-tutorial-e134f39564c7

import argparse
import apache_beam  as beam

def run(argv=None):
    # Crates a new instance of the ArgumentParser class
    parser = argparse.ArgumentParser()

    # Adds an argument to listen for
    parser.add_argument(
        '--input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt')
    parser.add_argument(
        '--output',
        required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)
    # CONTINUE HERE...
    
    with beam.Pipeline() as p:
        print("Hello World")
    
if __name__ == '__main__':
    run()
