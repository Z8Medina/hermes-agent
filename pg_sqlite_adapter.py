import os
import sys
import re
import threading
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Exception Classes (matching sqlite3)
# ---------------------------------------------------------------------------
class Error(Exception):
    pass

class OperationalError(Error):
    pass

class IntegrityError(Error):
    pass

def translate_exception(exc):
    exc_msg = str(exc).lower()
    if "unique constraint" in exc_msg or "duplicate key" in exc_msg:
        return IntegrityError(str(exc))
    return OperationalError(str(exc))

# ---------------------------------------------------------------------------
# Row class (subclass of tuple with dict-like keys access)
# ---------------------------------------------------------------------------
class Row(tuple):
    def __new__(cls, values, description):
        obj = super(Row, cls).__new__(cls, values)
        obj._keys = [d[0] for d in description] if description else []
        obj._key_to_idx = {name: idx for idx, name in enumerate(obj._keys)}
        return obj

    def __getitem__(self, key):
        if isinstance(key, str):
            if key in self._key_to_idx:
                return super().__getitem__(self._key_to_idx[key])
            raise KeyError(key)
        return super().__getitem__(key)

    def keys(self):
        return self._keys

# ---------------------------------------------------------------------------
# SQL Translation Helpers
# ---------------------------------------------------------------------------
def translate_params(sql):
    result = []
    in_quote = None
    escape = False
    for char in sql:
        if escape:
            result.append(char)
            escape = False
            continue
        if char == '\\':
            result.append(char)
            escape = True
            continue
        if char in ("'", '"'):
            if in_quote == char:
                in_quote = None
            elif in_quote is None:
                in_quote = char
        elif char == '?' and in_quote is None:
            result.append('%s')
            continue
        result.append(char)
    return "".join(result)

def translate_insert_ignore(sql):
    if re.search(r'INSERT\s+OR\s+IGNORE\s+INTO', sql, re.IGNORECASE):
        sql = re.sub(r'INSERT\s+OR\s+IGNORE\s+INTO', 'INSERT INTO', sql, flags=re.IGNORECASE)
        sql = sql.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
    return sql

def translate_insert_replace(sql):
    match = re.search(r'INSERT\s+OR\s+REPLACE\s+INTO\s+"?(\w+)"?\s*\((.*?)\)\s*VALUES\s*\((.*?)\)', sql, re.IGNORECASE)
    if match:
        table = match.group(1)
        cols_str = match.group(2)
        vals_str = match.group(3)
        cols = [c.strip().replace('"', '').replace("'", "") for c in cols_str.split(',')]
        pk = cols[0]
        update_cols = cols[1:]
        set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
        new_sql = f'INSERT INTO "{table}" ({cols_str}) VALUES ({vals_str}) ON CONFLICT ("{pk}") DO UPDATE SET {set_clause}'
        return new_sql
    return sql

def translate_match_queries(sql):
    if "messages_fts" in sql:
        sql = sql.replace("messages_fts MATCH ?", "m.content ILIKE %s")
        sql = sql.replace("messages_fts MATCH %s", "m.content ILIKE %s")
        sql = sql.replace("messages_fts_trigram MATCH ?", "m.content ILIKE %s")
        sql = sql.replace("messages_fts_trigram MATCH %s", "m.content ILIKE %s")
        sql = sql.replace("FROM messages_fts JOIN messages m ON m.id = messages_fts.rowid", "FROM messages m")
        sql = sql.replace("FROM messages_fts_trigram JOIN messages m ON m.id = messages_fts_trigram.rowid", "FROM messages m")
        sql = sql.replace("snippet(messages_fts, 0, '>>>', '<<<', '...', 40) AS snippet", "m.content AS snippet")
        sql = sql.replace("snippet(messages_fts_trigram, 0, '>>>', '<<<', '...', 40) AS snippet", "m.content AS snippet")
    return sql

def translate_sql(sql):
    sql_stripped = sql.strip().rstrip(';')
    
    # Check for PRAGMA table_info
    pragma_match = re.search(r'PRAGMA\s+table_info\s*\(\s*["\']?(\w+)["\']?\s*\)', sql_stripped, re.IGNORECASE)
    if pragma_match:
        table_name = pragma_match.group(1)
        new_sql = f"""
        SELECT 
            (ordinal_position - 1)::INTEGER AS cid,
            column_name::TEXT AS name,
            data_type::TEXT AS type,
            CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END::INTEGER AS notnull,
            column_default::TEXT AS dflt_value,
            CASE WHEN column_name = 'id' THEN 1 ELSE 0 END::INTEGER AS pk
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        return new_sql

    sql_trans = translate_params(sql_stripped)
    sql_trans = translate_insert_ignore(sql_trans)
    sql_trans = translate_insert_replace(sql_trans)
    sql_trans = translate_match_queries(sql_trans)
    
    # Auto RETURNING for serial primary key tables
    if sql_trans.strip().upper().startswith("INSERT") and "RETURNING" not in sql_trans.upper():
        table_match = re.search(r'INSERT\s+INTO\s+"?(\w+)"?', sql_trans, re.IGNORECASE)
        if table_match:
            tbl = table_match.group(1).lower()
            if tbl in ('messages', 'task_comments', 'task_events', 'task_runs'):
                sql_trans += " RETURNING id"
                
    return sql_trans

# ---------------------------------------------------------------------------
# PostgreSQL Connection and Cursor wrappers
# ---------------------------------------------------------------------------
class PGCursor:
    def __init__(self, connection):
        self.connection = connection
        self._cursor = connection.pg_conn.cursor()
        self.rowcount = -1
        self._lastrowid = None

    @property
    def description(self):
        return self._cursor.description

    @property
    def lastrowid(self):
        return self._lastrowid

    def execute(self, sql, parameters=None):
        if parameters is None:
            parameters = ()
        elif not isinstance(parameters, (tuple, list)):
            parameters = (parameters,)
            
        sql_trans = translate_sql(sql)
        
        # Format FTS search parameters for ILIKE wildcard mapping
        if "ILIKE" in sql_trans:
            new_params = []
            for p in parameters:
                if isinstance(p, str):
                    p_clean = p.replace('*', '').replace('"', '').replace("'", "").strip()
                    if not p_clean.startswith('%') and not p_clean.endswith('%'):
                        new_params.append(f"%{p_clean}%")
                    else:
                        new_params.append(p_clean)
                else:
                    new_params.append(p)
            parameters = tuple(new_params)
            
        try:
            self._cursor.execute(sql_trans, parameters)
            
            # Extract returned serial ID if applicable
            if "RETURNING id" in sql_trans:
                row = self._cursor.fetchone()
                if row:
                    self._lastrowid = row[0]
            
            self.rowcount = self._cursor.rowcount
        except Exception as e:
            raise translate_exception(e)
        return self

    def executescript(self, script):
        statements = script.split(';')
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
                
            stmt_upper = stmt.upper()
            if any(x in stmt_upper for x in (
                "CREATE VIRTUAL TABLE", 
                "USING FTS5", 
                "CREATE TRIGGER", 
                "DROP TRIGGER", 
                "PRAGMA JOURNAL_MODE", 
                "PRAGMA SYNCHRONOUS",
                "PRAGMA FOREIGN_KEYS",
                "PRAGMA WAL_CHECKPOINT"
            )):
                continue
                
            stmt_trans = stmt
            stmt_trans = re.sub(r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT", "BIGSERIAL PRIMARY KEY", stmt_trans, flags=re.IGNORECASE)
            stmt_trans = re.sub(r"INTEGER\s+PRIMARY\s+KEY", "BIGSERIAL PRIMARY KEY", stmt_trans, flags=re.IGNORECASE)
            stmt_trans = re.sub(r"\bINTEGER\b", "BIGINT", stmt_trans, flags=re.IGNORECASE)
            stmt_trans = translate_sql(stmt_trans)
            
            try:
                self._cursor.execute(stmt_trans)
            except Exception as e:
                err_msg = str(e).lower()
                if "already exists" in err_msg:
                    continue
        return self

    def fetchone(self):
        try:
            row = self._cursor.fetchone()
            if row is None:
                return None
            if self.connection.row_factory:
                return self.connection.row_factory(row, self._cursor.description)
            return row
        except Exception as e:
            raise translate_exception(e)

    def fetchall(self):
        try:
            rows = self._cursor.fetchall()
            if self.connection.row_factory:
                return [self.connection.row_factory(r, self._cursor.description) for r in rows]
            return rows
        except Exception as e:
            raise translate_exception(e)

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                break
            yield row

    def close(self):
        self._cursor.close()

class PGConnection:
    def __init__(self, dsn):
        self._dsn = dsn
        self._local = threading.local()
        self._row_factory = None
        self.isolation_level = None

    @property
    def pg_conn(self):
        if not getattr(self._local, 'conn', None) or self._local.conn.closed:
            self._local.conn = psycopg2.connect(self._dsn)
            self._local.conn.autocommit = False
        return self._local.conn

    @property
    def row_factory(self):
        return self._row_factory

    @row_factory.setter
    def row_factory(self, val):
        self._row_factory = val

    def cursor(self):
        return PGCursor(self)

    def execute(self, sql, parameters=None):
        cur = self.cursor()
        cur.execute(sql, parameters)
        return cur

    def executescript(self, script):
        cur = self.cursor()
        cur.executescript(script)
        return cur

    def commit(self):
        try:
            self.pg_conn.commit()
        except Exception as e:
            raise translate_exception(e)

    def rollback(self):
        try:
            self.pg_conn.rollback()
        except Exception:
            pass

    def close(self):
        conn = getattr(self._local, 'conn', None)
        if conn and not conn.closed:
            conn.close()

# ---------------------------------------------------------------------------
# Global connect function redirecting to PostgreSQL if DATABASE_URL is set
# ---------------------------------------------------------------------------
# Load standard sqlite3 module dynamically to fall back on it
sys_sqlite3_orig = sys.modules.pop('sqlite3', None)
try:
    import sqlite3 as _real_sqlite3
finally:
    if sys_sqlite3_orig is not None:
        sys.modules['sqlite3'] = sys_sqlite3_orig
    else:
        sys.modules['sqlite3'] = _real_sqlite3

def connect(database, *args, **kwargs):
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return PGConnection(dsn)
    else:
        return _real_sqlite3.connect(database, *args, **kwargs)
