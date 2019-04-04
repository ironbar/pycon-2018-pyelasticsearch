from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tqdm import tqdm

from searchapp.constants import DOC_TYPE, INDEX_NAME
from searchapp.data import all_products, ProductData


def main():
    # Connect to localhost:9200 by default.
    es = Elasticsearch()

    es.indices.delete(index=INDEX_NAME, ignore=404)
    es.indices.create(
        index=INDEX_NAME,
        body={
            'mappings': {
                DOC_TYPE: {                                   # This mapping applies to products.
                    'properties': {                             # Just a magic word.
                        'name': {                                 # The field we want to configure.
                            'type': 'text',                         # The kind of data we’re working with.
                            'fields': {                             # create an analyzed field.
                                'english_analyzed': {                 # Name that field `name.english_analyzed`.
                                    'type': 'text',                     # It’s also text.
                                    'analyzer': 'english',              # And here’s the analyzer we want to use.
                                }
                            }
                        }
                    }
                }
            },
            'settings': {},
        },
    )
    products_to_index(es)
    # for product in tqdm(all_products(), desc='Indexing products'):
    #     index_product(es, product)


def index_product(es, product: ProductData):
    """Add a single product to the ProductData index."""

    es.create(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        id=product.id,
        body={
            "name": product.name,
            "image": product.image,
            # "description": product.description,
            # "price": product.price,
            # "taxonomy": product.taxonomy
        }
    )

def products_to_index(es):
    bulk(es, gendata())

def gendata():
    for product in all_products():
        yield dict(
            _index=INDEX_NAME,
            _type=DOC_TYPE,
            _id=product.id,
            _source={
                "name": product.name,
                "image": product.image,
            }
        )


if __name__ == '__main__':
    main()
