# queries.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.database.utils import get_kind


def get_start_query(subj, pred, obj, table_name, fetch_size=500):
    """
        Get a prepared SQL query which starts scanning for a triple pattern
        and the parameters used to execute it.
    """
    kind = get_kind(subj, pred, obj)
    query = "SELECT * FROM {} ".format(table_name)
    params = []
    if kind == 'spo':
        query += "WHERE subject = ? AND predicate = ? AND object = ? ORDER BY subject, predicate, object"
        params = (subj, pred, obj)
    elif kind == '???':
        # query += ' ORDER BY subject, predicate, object'
        query += ' ORDER BY predicate, object, subject'
    elif kind == 's??':
        query += "WHERE subject = ? ORDER BY subject, predicate, object"
        params = [subj]
    elif kind == 'sp?':
        query += "WHERE subject = ? AND predicate = ? ORDER BY subject, predicate, object"
        params = (subj, pred)
    elif kind == '?p?':
        query += "WHERE predicate = ? ORDER BY predicate, object, subject"
        params = [pred]
    elif kind == '?po':
        query += "WHERE predicate = ? AND object = ? ORDER BY predicate, object, subject"
        params = (pred, obj)
    elif kind == 's?o':
        query += "WHERE subject = ? AND object = ? ORDER BY object, subject, predicate"
        params = (subj, obj)
    elif kind == '??o':
        query += "WHERE object = ? ORDER BY object, subject, predicate"
        params = [obj]
    else:
        raise Exception("Unkown pattern type: {}".format(kind))
    return query, params


def get_resume_query(subj, pred, obj, last_read, table_name, fetch_size=500, symbol=">="):
    """
        Get a prepared SQL query which resumes scanning for a triple pattern
        and the parameters used to execute it.
    """
    last_s, last_p, last_o = last_read
    kind = get_kind(subj, pred, obj)
    query = "SELECT * FROM {} ".format(table_name)
    params = None
    if kind == 'spo':
        return None, None
    elif kind == '???':
        # query += f"WHERE (subject, predicate, object) {symbol} (?, ?, ?) ORDER BY subject, predicate, object"
        # params = (last_s, last_p, last_o)
        query += f"WHERE (predicate, object, subject) {symbol} (?, ?, ?) ORDER BY predicate, object, subject"
        params = (last_p, last_o, last_s)
    elif kind == 's??':
        query += f"WHERE subject = ? AND (predicate, object) {symbol} (?, ?) ORDER BY subject, predicate, object"
        params = (last_s, last_p, last_o)
    elif kind == 'sp?':
        query += f"WHERE subject = ? AND predicate = ? AND (object) {symbol} (?) ORDER BY subject, predicate, object"
        params = (last_s, last_p, last_o)
    elif kind == '?p?':
        query += f"WHERE predicate = ? AND (object, subject) {symbol} (?, ?) ORDER BY predicate, object, subject"
        params = (last_p, last_o, last_s)
    elif kind == '?po':
        query += f"WHERE predicate = ? AND object = ? AND (subject) {symbol} (?) ORDER BY predicate, object, subject"
        params = (last_p, last_o, last_s)
    elif kind == 's?o':
        query += f"WHERE subject = ? AND object = ? AND (predicate) {symbol} (?) ORDER BY object, subject, predicate"
        params = (last_s, last_o, last_p)
    elif kind == '??o':
        query += f"WHERE object = ? AND (subject, predicate) {symbol} (?, ?) ORDER BY object, subject, predicate"
        params = (last_o, last_s, last_p)
    else:
        raise Exception("Unkown pattern type: {}".format(kind))
    return query, params


def get_insert_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return "INSERT INTO {} (subject,predicate,object) VALUES (?,?,?) ON CONFLICT (subject,predicate,object) DO NOTHING".format(
        table_name)


def get_insert_many_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return "INSERT INTO {} (subject,predicate,object) VALUES ? ON CONFLICT (subject,predicate,object) DO NOTHING".format(
        table_name)


def get_delete_query(table_name):
    """Build a SQL query to delete a RDF triple form a PostgreSQL dataset"""
    return "DELETE FROM {} WHERE subject = ? AND predicate = ? AND object = ?".format(table_name)
