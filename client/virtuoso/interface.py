import click
import logging
import json

import http_client as http
import sys

logger = logging.getLogger(__name__)

@click.group()
def cli():
    pass  

@cli.command()
@click.argument("server")
@click.argument("dataset")
@click.option("--query", type=click.STRING)
@click.option("--file", type=click.Path(exists=True))
@click.option("--measure", type=click.Path())
@click.option("--format", type=click.Choice(["w3c/json", "w3c/xml"]))
@click.option("--output", type=click.STRING)
@click.option("--display", is_flag=True, default=False)
def query(server, dataset, query, file, measure, format, output, display):
    # Extracts the query from the arguments
    if file is not None:
        query = open(file, 'r').read()
    if query is None:
        logger.error('You have to input a SPARQL query')
        sys.exit(1)
    # Evaluates the query
    result, stats = http.execute(server, dataset, query, format)
    # Writes the query result
    if measure is not None:
        with open(measure, 'w') as out_file:
            out_file.write(f'{stats.execution_time()},{stats.nb_calls()},{stats.data_transfer()}')
    if display:
        logger.info(result)
    if output is not None:
        with open(output, 'w') as out_file:
            out_file.write(result)
    logger.info(f'Evaluation metrics: \
        \ntime: {stats.execution_time()} sec \
        \ntransfer: {stats.data_transfer()} bytes \
        \ncalls: {stats.nb_calls()} \
        \nresult: {stats.nb_result()}')

if __name__ == "__main__":
    cli()