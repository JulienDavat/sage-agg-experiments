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
    params = None
    if kind == 'spo':
        query += "WHERE subject = %s AND predicate = %s AND object = %s ORDER BY md5(subject), md5(predicate), md5(object)"
        params = (subj, pred, obj)
    elif kind == '???':
        query += ' ORDER BY md5(subject), md5(predicate), md5(object)'
    elif kind == 's??':
        query += "WHERE md5(subject) = md5(%s) ORDER BY md5(subject), md5(predicate), md5(object)"
        params = [subj]
    elif kind == 'sp?':
        query += "WHERE md5(subject) = md5(%s) AND md5(predicate) = %s ORDER BY md5(subject), md5(predicate), md5(object)"
        params = (subj, pred)
    elif kind == '?p?':
        query += "WHERE md5(predicate) = md5(%s) ORDER BY md5(predicate), md5(object), md5(subject)"
        params = [pred]
    elif kind == '?po':
        query += "WHERE md5(predicate) = md5(%s) AND md5(object) = md5(%s) ORDER BY md5(predicate), md5(object), md5(subject)"
        params = (pred, obj)
    elif kind == 's?o':
        query += "WHERE md5(subject) = md5(%s) AND md5(object) = md5(%s) ORDER BY md5(object), md5(subject), md5(predicate)"
        params = (subj, obj)
    elif kind == '??o':
        query += "WHERE md5(object) = md5(%s) ORDER BY md5(object), md5(subject), md5(predicate)"
        params = [obj]
    else:
        raise Exception("Unkown pattern type: {}".format(kind))
    query += " LIMIT {}".format(fetch_size)
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
        query += "WHERE (md5(subject), md5(predicate), md5(object)) {} (md5(%s), md5(%s), md5(%s)) ORDER BY md5(subject), md5(predicate), md5(object)".format(symbol)
        params = (last_s, last_p, last_o)
    elif kind == 's??':
        query += "WHERE md5(subject) = md5(%s) AND (md5(predicate), md5(object)) {} (md5(%s), md5(%s)) ORDER BY md5(subject), md5(predicate), md5(object)".format(symbol)
        params = (last_s, last_p, last_o)
    elif kind == 'sp?':
        query += "WHERE md5(subject) = md5(%s) AND md5(predicate) = md5(%s) AND (md5(object)) {} (md5(%s)) ORDER BY md5(subject), md5(predicate), md5(object)".format(symbol)
        params = (last_s, last_p, last_o)
    elif kind == '?p?':
        query += "WHERE md5(predicate) = md5(%s) AND (md5(object), md5(subject)) {} (md5(%s), md5(%s)) ORDER BY md5(predicate), md5(object), md5(subject)".format(symbol)
        params = (last_p, last_o, last_s)
    elif kind == '?po':
        query += "WHERE md5(predicate) = md5(%s) AND md5(object) = md5(%s) AND (md5(subject)) {} (md5(%s)) ORDER BY md5(predicate), md5(object), md5(subject)".format(symbol)
        params = (last_p, last_o, last_s)
    elif kind == 's?o':
        query += "WHERE md5(subject) = md5(%s) AND md5(object) = md5(%s) AND (md5(predicate)) {} (md5(%s)) ORDER BY md5(object), md5(subject), md5(predicate)".format(symbol)
        params = (last_s, last_o, last_p)
    elif kind == '??o':
        query += "WHERE md5(object) = md5(%s) AND (md5(subject), md5(predicate)) {} (md5(%s), md5(%s)) ORDER BY md5(object), md5(subject), md5(predicate)".format(symbol)
        params = (last_o, last_s, last_p)
    else:
        raise Exception("Unkown pattern type: {}".format(kind))
    query += " LIMIT {}".format(fetch_size)
    return query, params


def get_insert_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return "INSERT INTO {} (subject,predicate,object) VALUES (%s,%s,%s) ON CONFLICT (subject,predicate,object) DO NOTHING".format(table_name)


def get_insert_many_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return "INSERT INTO {} (subject,predicate,object) VALUES %s ON CONFLICT (subject,predicate,object) DO NOTHING".format(table_name)


def get_delete_query(table_name):
    """Build a SQL query to delete a RDF triple form a PostgreSQL dataset"""
    return "DELETE FROM {} WHERE subject = %s AND predicate = %s AND object = %s".format(table_name)
