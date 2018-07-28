from sqlalchemy import create_engine


def get_results_cursor(sql_bind, query, pool_timeout=300):
    engine = create_engine(sql_bind, pool_timeout=pool_timeout)
    connection = engine.connect()
    return connection.execute(query)
