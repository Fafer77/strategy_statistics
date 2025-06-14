import argparse
import questionary
import psycopg
from psycopg.errors import UniqueViolation


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS strategies (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            description TEXT            
        );            
""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );                    
""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            strategy_id INTEGER NOT NULL REFERENCES strategies(id)
                        ON DELETE RESTRICT,
            instrument_id INTEGER NOT NULL REFERENCES instruments(id)
                        ON DELETE RESTRICT,
            trade_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            result NUMERIC,
            description TEXT,
            emotions TEXT,
            time_in_trade NUMERIC CHECK (time_in_trade >= 0)
        );  
""")
        conn.commit()


def new_strategy(conn):
    strategy_name = questionary.text('Name your new strategy: ').ask()
    desc = questionary.text('Provide description for your strategy: ').ask()
    
    with conn.cursor() as cur:
        try:
            cur.execute(
            "INSERT INTO strategies(name, description) VALUES (%s, %s)",
            (strategy_name, desc)
        )
            conn.commit()
            print(f'Strategy {strategy_name} added.')
        except UniqueViolation:
            conn.rollback()
            print(f'Strategy {strategy_name} already exists.')
        except psycopg.Error as e:
            conn.rollback()
            print(f'Error {e} occurred.')


def delete_strategy(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM strategies;")
        rows = cur.fetchall()
        strategy_names = [row[0] for row in rows]

        if not strategy_names:
            print('There are no strategies saved in database.')
            return
        
        to_delete = questionary.select(
            "Choose strategy to delete: ",
            choices=strategy_names
        ).ask()

        if to_delete is None:
            print("No strategy was chosen.")
            return

        cur.execute("DELETE FROM strategies WHERE name = %s;", (to_delete,))
    
    conn.commit()
    print(f'Strategy {to_delete} was deleted.')


def print_strategies(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * from strategies;                    
""")
        rows = cur.fetchall()
        for row in rows:
            print(row)


def insert(conn):
    cursor = conn.cursor()

def modify(conn):
    pass

def delete(conn):
    pass

def history(conn):
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Financial markets strategy statistics collector")
    parser.add_argument('operation', 
                        choices=['insert', 'modify', 'delete', 'history', 
                                 'quit', 'insert-strategy', 'init-db', 'print-strategies',
                                 'delete-strategy'],
                        help='Operation to choose')
    args = parser.parse_args()
    conn = psycopg.connect(dbname="strategystats", user="postgres",
                           password="toor", port=5432, host='127.0.0.1')
    
    if args.operation == 'insert':
        insert(conn)
    elif args.operation == 'modify':
        modify(conn)
    elif args.operation == 'insert-strategy':
        new_strategy(conn)
    elif args.operation == 'print-strategies':
        print_strategies(conn)
    elif args.operation == 'delete-strategy':
        delete_strategy(conn)
    elif args.operation == 'delete':
        delete(conn)
    elif args.operation == 'history':
        history(conn)
    elif args.operation == 'init-db':
        init_db(conn)
        print('Database initiated!')
    else:
        print('Goodbye!')
    
    conn.close()

