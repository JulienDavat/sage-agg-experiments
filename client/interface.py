import click
import logging
import json

import http_client as http
import engine
import sys

from formatters import sparql_json, sparql_xml

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
@click.option("--format", type=click.Choice(["w3c/json", "w3c/xml", "json"]))
@click.option("--output", type=click.STRING)
def query(query, file, dataset, measure, format, output):
    logger.info(query)
    if file is not None:
        query = open(file, 'r').read()
    if query is None:
        logger.error('You have to input a SPARQL query')
        sys.exit(1)
    if dataset is None:
        logger.error('You have to specified a dataset')
        sys.exit(1)
    result, stats = engine.execute(query, dataset)
    if measure is not None:
        with open(measure, 'w') as out_file:
            out_file.write(f'{stats.execution_time()},{stats.nb_calls()},{stats.data_transfer()}')
    if output is not None:
        with open(output, 'w') as out_file:
            if format == 'w3c/json':
                out_file.write(sparql_json(result.get()))
            elif format == 'wc3/xml':
                out_file.write(sparql_xml(result.get()))
            elif format == 'json':
                out_file.write(result.get())
            else:
                out_file.write(result.get())
    logger.info(f'Evaluation metrics: \
        \ntime: {stats.execution_time()} sec \
        \ntransfer: {stats.data_transfer()} bytes \
        \ncalls: {stats.nb_calls()} \
        \nresult: {stats.nb_result()}')

if __name__ == "__main__":
    cli()