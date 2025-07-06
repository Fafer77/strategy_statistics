import argparse
import questionary
from questionary import Choice
import psycopg
from psycopg.errors import UniqueViolation
import datetime
from decimal import Decimal
import pandas as pd
from matplotlib import pyplot


def _validate_datetime(s: str) -> bool:
    try:
        datetime.datetime.strptime(s, "%Y-%m-%d %H:%M")
        return True
    except ValueError:
        return False


def _validate_numeric(s: str) -> bool:
    try:
        Decimal(s)
        return True
    except:
        return False


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
            initial_risk NUMERIC CHECK (initial_risk >= 0),
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
        cur.execute("SELECT * from strategies;")
        rows = cur.fetchall()
        for row in rows:
            print(row)
    

def insert(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM strategies;")
        strategies = cur.fetchall()

        cur.execute("SELECT id, name FROM instruments;")
        instruments = cur.fetchall()

    if not strategies:
        print('First add your strategy.')
        return
    
    if not instruments:
        print('First add instruments.')
        return

    strategy = questionary.select(
        "Choose strategy: ",
        choices=[Choice(title=name, value=id_) for id_, name in strategies]
    ).ask()

    instrument = questionary.select(
        "Choose instrument: ",
        choices=[Choice(title=name, value=id_) for id_, name in instruments]
    ).ask()
    
    date_str = questionary.text(
        "Enter trade time (YYYY-MM-DD HH:MM):",
        validate=lambda t: True if _validate_datetime(t) else "Invalid format, use YYYY-MM-DD HH:MM"
    ).ask()
    if date_str is None:
        print("Cancelled.")
        return
    trade_time = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M")

    result_str = questionary.text(
        "Enter your profit/loss (+/-) on this trade:",
        validate=lambda x: True if _validate_numeric(x) else "Must be a number"
    ).ask()
    if result_str is None:
        print("Cancelled.")
        return
    result = Decimal(result_str)

    initial_risk_str = questionary.text(
        "Enter initial risk (amount you risked):",
        validate=lambda x: True if _validate_numeric(x) else "Must be a number"
    ).ask()
    if initial_risk_str is None:
        print("Cancelled.")
        return
    initial_risk = Decimal(initial_risk_str)

    description = questionary.text(
        "Why did you enter this trade? Were there any events today?"
    ).ask()

    emotions = questionary.text(
        "How did you feel when entering, during, and after closing the trade?"
    ).ask()

    time_str = questionary.text(
        "Enter time in trade (minutes):",
        validate=lambda x: True if _validate_numeric(x) else "Must be a number"
    ).ask()
    if time_str is None:
        print("Cancelled.")
        return
    time_in_trade = Decimal(time_str)

    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO trades (
            strategy_id, instrument_id, trade_time,
            result, initial_risk,
            description, emotions, time_in_trade
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            strategy, instrument, trade_time,
            result, initial_risk,
            description, emotions, time_in_trade
        ))
        conn.commit()
        print("Trade added successfully.")


def insert_instrument(conn):
    instrument = questionary.text("Name for instrument: ").ask()

    with conn.cursor() as cur:
        try:
            cur.execute(
                "INSERT INTO instruments (name) VALUES (%s);", (instrument,)
            )
            conn.commit()
            print('Instrument added successfully!')
        except UniqueViolation:
            conn.rollback()
            print(f'Instrument with name {instrument} already exists')
        except psycopg.Error as e:
            conn.rollback()
            print(f'Adding instrument failed. Error {e}.')


def delete_instrument(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM instruments;")
        instruments = cur.fetchall()
    
    if not instruments:
        print('There are no instruments.')
        return
    
    to_delete = questionary.select(
        "Select instrument to delete:",
        choices=[Choice(title=name, value=id_) for id_, name in instruments]
    ).ask()
    
    with conn.cursor() as cur:
        cur.execute("DELETE FROM instruments WHERE id = %s", (to_delete,))
        conn.commit()
        print('Instrument deleted successfully.')


def modify(conn):
    id_str = questionary.text("Enter trade id you want to modify:").ask()
    try:
        trade_id = int(id_str)
    except ValueError:
        print('You should enter a number.')
        return
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT strategy_id, instrument_id, trade_time,
                   result, initial_risk,
                   description, emotions, time_in_trade
            FROM trades
            WHERE id = %s
        """, (trade_id,))
        row = cur.fetchone()
    
    if row is None:
        print(f'There is no trade with id {trade_id}')
        return
    
    (curr_strat, curr_instr, curr_time,
     curr_result, curr_initial,
     curr_desc, curr_emot, curr_dur) = row

    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM strategies;")
        strategies = cur.fetchall()
    strategy_id = questionary.select(
        "Choose strategy:",
        choices=[Choice(title=name, value=id_) for id_, name in strategies],
        default=str(curr_strat)
    ).ask()
    if strategy_id is None:
        print("Cancelled.")
        return

    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM instruments;")
        instruments = cur.fetchall()
    instrument_id = questionary.select(
        "Choose instrument:",
        choices=[Choice(title=name, value=id_) for id_, name in instruments],
        default=str(curr_instr)
    ).ask()
    if instrument_id is None:
        print("Cancelled.")
        return

    new_time_str = questionary.text(
        "Enter new trade time (YYYY-MM-DD HH:MM):",
        default=curr_time.strftime("%Y-%m-%d %H:%M")
    ).ask()
    if new_time_str is None:
        print("Cancelled.")
        return
    new_time = datetime.datetime.strptime(new_time_str, "%Y-%m-%d %H:%M")

    new_result_str = questionary.text(
        "Enter new profit/loss (+/-):",
        default=str(curr_result) if curr_result is not None else ""
    ).ask()
    if new_result_str is None:
        print("Cancelled.")
        return
    new_result = Decimal(new_result_str) if new_result_str else None

    new_initial_str = questionary.text(
        "Enter new initial risk:",
        default=str(curr_initial) if curr_initial is not None else "",
        validate=lambda x: True if _validate_numeric(x) else "Must be a number"
    ).ask()
    if new_initial_str is None:
        print("Cancelled.")
        return
    new_initial = Decimal(new_initial_str) if new_initial_str else None

    new_desc = questionary.text(
        "Enter new description:",
        default=curr_desc or ""
    ).ask()
    if new_desc is None:
        print("Cancelled.")
        return

    new_emot = questionary.text(
        "Enter new emotions:",
        default=curr_emot or ""
    ).ask()
    if new_emot is None:
        print("Cancelled.")
        return

    new_dur_str = questionary.text(
        "Enter new time in trade (minutes):",
        default=str(curr_dur) if curr_dur is not None else ""
    ).ask()
    if new_dur_str is None:
        print("Cancelled.")
        return
    new_dur = Decimal(new_dur_str) if new_dur_str else None

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE trades SET
                strategy_id   = %s,
                instrument_id = %s,
                trade_time    = %s,
                result        = %s,
                initial_risk  = %s,
                description   = %s,
                emotions      = %s,
                time_in_trade = %s
            WHERE id = %s;
        """, (
            strategy_id, instrument_id, new_time,
            new_result, new_initial, new_desc, new_emot, new_dur,
            trade_id
        ))
    conn.commit()
    print(f"Trade {trade_id} has been updated.")


def delete(conn):
    id_str = questionary.text("Enter trade id you want to delete: ").ask()
    try:
        delete_id = int(id_str)
    except ValueError:
        print('You should enter a number.')
        return
    
    with conn.cursor() as cur:
        cur.execute("DELETE FROM trades WHERE id = %s", (delete_id,))
        conn.commit()
        print(f'Trade with id {delete_id} was deleted')


def history(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM strategies;")
        strategies = cur.fetchall()

    if not strategies:
        print('There are no strategies.')
        return
    
    strategy = questionary.select(
        "Choose strategy to display history",
        choices=[Choice(title=name, value=id_) for id_, name in strategies]
    ).ask()
    
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM trades t
        JOIN strategies s ON t.strategy_id = s.id
        WHERE t.strategy_id = %s;
        """, (strategy,))
        trade_history = cur.fetchall()

    for trade in trade_history:
        print(trade)


# statistics functions
def calculate_win_rate(series):
    wins = (series >= 0).sum()
    return wins / len(series)


def average_time_in_trade(series):
    return series.mean()


def calc_profit(series):
    return series.sum()


def show_stats(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM strategies")
        strategies = cur.fetchall()
        cur.execute("SELECT id, name FROM instruments")
        instruments = cur.fetchall()
    
    if not strategies or not instruments:
        print('There are no strategies or instruments in database')
        return
    
    strategy_choices = [Choice(value=id_, title=name_) for id_, name_ in strategies]
    strategy_choices.append(Choice(value=None, title='all'))
    strategy_id = questionary.select('Choose strategy: ', choices=strategy_choices).ask()
    
    instrument_choices = [Choice(value=id_, title=name_) for id_, name_ in instruments]
    instrument_choices.append(Choice(value=None, title='all'))
    instrument_id = questionary.select('Choose instrument: ', choices=instrument_choices).ask()

    sql = "SELECT * FROM trades"
    params = []
    if strategy_id != 'all':
        sql += " WHERE strategy_id = %s"
        params.append(strategy_id)
    if instrument_id != 'all':
        prefix = " AND" if params else " WHERE"
        sql += f"{prefix} instrument_id = %s"
        params.append(instrument_id)

    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

    if not rows:
        print("No records meet this criteria.")
        return
    
    df = pd.DataFrame(rows, columns=col_names)
    
    win_rate = round(calculate_win_rate(df['result']) * 100, 2)
    average_trade_time = round(average_time_in_trade(df['time_in_trade']), 2)
    profit = round(calc_profit(df['result']), 2)
    avg_initial_risk = round(df['initial_risk'].mean(), 2)
    expected_value = round(profit / len(df), 2)
    risk_reward_ratio = round((df['result'] / df['initial_risk']).mean(), 2)

    print(df.to_string())

    print(f"Win-rate:               {win_rate}%")
    print(f"Avg time in trade:      {average_trade_time} min")
    print(f"Total P/L:              {profit}")
    print(f"Avg initial risk:       {avg_initial_risk}")
    print(f"Expected value/trade:   {expected_value}")
    print(f"Avg risk-reward ratio:  {risk_reward_ratio}\n") 


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Financial markets strategy statistics collector"
    )
    parser.add_argument(
        'operation', 
        choices=[
            'insert', 'modify', 'delete', 'history', 'quit',
            'insert-strategy', 'init-db', 'print-strategies', 'delete-strategy',
            'insert-instrument', 'delete-instrument', 'show-stats'
        ],
        help='Operation to choose'
    )
    args = parser.parse_args()
    conn = psycopg.connect(
        dbname="strategystats",
        user="postgres",
        password="toor",
        port=5432,
        host='127.0.0.1'
    )
    
    if args.operation == 'insert':
        insert(conn)
    elif args.operation == 'modify':
        modify(conn)
    elif args.operation == 'delete':
        delete(conn)
    elif args.operation == 'history':
        history(conn)
    elif args.operation == 'show-stats':
        show_stats(conn)
    elif args.operation == 'insert-strategy':
        new_strategy(conn)
    elif args.operation == 'print-strategies':
        print_strategies(conn)
    elif args.operation == 'delete-strategy':
        delete_strategy(conn)
    elif args.operation == 'insert-instrument':
        insert_instrument(conn)
    elif args.operation == 'delete-instrument':
        delete_instrument(conn)
    elif args.operation == 'init-db':
        init_db(conn)
        print('Database initiated!')
    else:
        print('Goodbye!')
    
    conn.close()
