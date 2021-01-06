import click
import logging
import json

import http_client as http
import engine
import sys

logger = logging.getLogger()

@click.group()
def cli():
    pass  

@cli.command()
@click.option("--query", type=click.STRING)
@click.option("--file", type=click.Path(exists=True))
@click.option("--dataset", type = click.Choice(["bsbm1k"]),
    default = "bsbm1k", show_default = True, help = "The dataset that will be queried")
@click.option("--measure", type=click.Path())
def query(query, file, dataset, measure):
    print(query)
    if file is not None:
        query = open(file, 'r').read()
    if query is None:
        logger.error('You have to input a SPARQL query')
        sys.exit(1)
    if dataset is None:
        logger.error('You have to specified a dataset')
        sys.exit(1)
    result = engine.execute(query, dataset)
    if measure is not None:
        with open(measure, 'w') as out_file:
            out_file.write(f'{result.execution_time()},{result.nb_calls()},{result.data_transfer()}')

if __name__ == "__main__":
    cli()