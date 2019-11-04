#!/usr/bin/env python3
from neo4j import GraphDatabase
import csv
import sys

# Usage: python import.py bolt://host:port username password


class CdmhImporter(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def get_node_count(self):
        with self._driver.session() as session:
            count = session.write_transaction(self._count_nodes)
        return count

    def create_vocab(self, name, cdmh_id):
        with self._driver.session() as session:
            _node_id = session.write_transaction(self._create_vocab_node, name, cdmh_id)

    def create_code(self, code, short_name, long_name, cdmh_id, vocab_id, ncit_code=None):
        with self._driver.session() as session:
            node_id = session.write_transaction(self._create_code_node, code, short_name, long_name, cdmh_id, ncit_code)
        with self._driver.session() as session:
            session.write_transaction(self._add_vocab_code_relationship, node_id, vocab_id)

    def add_parent(self, child_id, parent_id):
        with self._driver.session() as session:
            session.write_transaction(self._add_parent_relationship, child_id, parent_id)

    def add_mapping(self, from_id, to_id):
        with self._driver.session() as session:
            session.write_transaction(self._add_code_mapping, from_id, to_id)

    @staticmethod
    def _count_nodes(tx):
        result = tx.run("MATCH (n) RETURN count(n) as count")
        return result.single()[0]

    @staticmethod
    def _create_code_node(tx, code, short_name, long_name, cdmh_id, ncit_code=None):
        return tx.run("CREATE (a:Code { value: $value, short_name: $short_name, long_name: $long_name, cdmh_id: $id, ncit_code: $ncit_code }) "
                      "RETURN id(a)",
                      short_name=short_name, long_name=long_name, id=cdmh_id, value=code, ncit_code=ncit_code).single().value()

    @staticmethod
    def _create_vocab_node(tx, name, vocab_id):
        return tx.run("CREATE (a:Vocabulary {name: $name, cdmh_id: $id}) RETURN id(a)", name=name, id=vocab_id).single().value()

    @staticmethod
    def _add_vocab_code_relationship(tx, code_node_id, vocab_cdmh_id):
        return tx.run("MATCH (a:Code), (b:Vocabulary) "
                      "WHERE ID(a)=$code_id AND b.cdmh_id=$vocab_id "
                      "CREATE (a)-[r:PARTOF]->(b) "
                      "RETURN r",
                      code_id=code_node_id,
                      vocab_id=vocab_cdmh_id)

    @staticmethod
    def _add_parent_relationship(tx, child_id, parent_id):
        return tx.run("MATCH (a:Code), (b:Code) "
                      "WHERE a.cdmh_id=$child_id AND b.cdmh_id=$parent_id "
                      "CREATE (a)-[r:HAS_PARENT]->(b) "
                      "RETURN r",
                      child_id=child_id,
                      parent_id=parent_id)

    @staticmethod
    def _add_code_mapping(tx, from_id, to_id):
        return tx.run("MATCH (a:Code), (b:Code) "
                      "WHERE a.cdmh_id=$from_id AND b.cdmh_id=$to_id "
                      "CREATE (a)-[r:MAP_TO]->(b) "
                      "RETURN r",
                      from_id=from_id,
                      to_id=to_id)


if len(sys.argv) != 4:
    print("Usage: python import.py bolt_url, username, password\n")
    exit(1)
importer = CdmhImporter(sys.argv[1], sys.argv[2], sys.argv[3])
# with open('data/Vocabulary.csv', 'r') as csvfile:
#     reader = csv.reader(csvfile, quotechar='"', delimiter=',', doublequote=True, quoting=csv.QUOTE_NONNUMERIC)
#     next(reader)
#     for row in reader:
#         importer.create_vocab(row[0], int(row[5]))
#
# # This uses the combined file created in explore.ipynb notebook. To use the original Code_Value files separately,
# # repeat this process for each file.
# # Note this could take a long time ( > 12 hours)
# with open('data/Code_Values_All.csv', 'r') as csvfile:
#     reader = csv.reader(csvfile, quotechar='"', delimiter=',', doublequote=True, quoting=csv.QUOTE_ALL)
#     next(reader)
#     for row in reader:
#         importer.create_code(row[0], row[2], row[3], row[7], int(row[8]), row[1])

with open('data/Code_Values_All.csv', 'r') as csvfile:
    reader = csv.reader(csvfile, quotechar='"', delimiter=',', doublequote=True, quoting=csv.QUOTE_ALL)
    next(reader)
    for row in reader:
        if row[9]:
            importer.add_parent(row[7], row[9])

with open('data/Code_Mapping.csv', 'r') as csvfile:
    reader = csv.reader(csvfile, quotechar='"', delimiter=',', doublequote=True, quoting=csv.QUOTE_MINIMAL)
    next(reader)
    for row in reader:
        importer.add_mapping(row[5], row[4])

importer.close()
