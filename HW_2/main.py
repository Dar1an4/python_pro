import sqlite3

from mimesis import Address, Finance, Person


"""
There are one unexpected error. 'near "________": syntax error'
Its rised due to an internal error of mimesis library, sometimes generate bad names
"""


person, address, finance = Person('en'), Address('en'), Finance('en')


def db_creator(db_name: str, table_name: str) -> None:
    """
    Creating our DB.
    :param db_name: If u need to create a new db or add table to exist db just input str new name of db
    :param table_name: enter str new name of table
    :return: None. Only print report about completed work.
    """
    try:
        with sqlite3.connect(db_name) as conn:
            cur = conn.cursor()
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
            personid INT PRIMARY KEY,
            first_name TEXT VARCHAR(128),
            last_name TEXT VARCHAR(128),
            address TEXT VARCHAR(1024),
            job TEXT VARCHAR(128),
            age INT VARCHAR(3));
            """)
        print('Mission completed. DB already exist or created')
    except Exception as err:
        print(f'Something wrong! error is: \n{err}')
    return


def db_max_personid(db_name='db-new.db', table_name='persons') -> int:
    """
    Just checking and return max personid
    :param db_name: str, default - our DB
    :param table_name: str, default - table `persons`
    :return: int, max personid in column personid
    """
    with sqlite3.connect(db_name) as conn:
        cur = conn.cursor()
        cur.execute(f"""
        SELECT MAX(personid) from {table_name}
        """)
        max_id = cur.fetchone()
        max_id = 0 if str(max_id[0]) == "None" else max_id[0]
        return max_id


def db_linecouter(db_name='db-new.db', table_name='persons') -> int:
    """
        Just checking and return how many lines are in the database table
    :param db_name: str, default - our DB
    :param table_name: str, default - table `persons`
    :return: int, how many lines of data are in our DB table
    """
    with sqlite3.connect(db_name) as conn:
        cur = conn.cursor()
        cur.execute(f"""
        SELECT COUNT(personid) from {table_name}
        """)
        counter = cur.fetchone()[0]
        return counter


def db_insert(first_name: str, last_name: str, address: str, job: str, age: int, personid=db_max_personid()+1,
              db_name='db-new.db', table_name='persons') -> None:
    """
    Need to INSERT person with his personal data into the database table
    :param db_name: str, default - our DB
    :param table_name: str, default - table `persons`
    :param first_name: str, first name of the person
    :param last_name: str, last name of the person
    :param address: str, address of the person
    :param job: str, job (occupation) of the person
    :param age: int, age of the person
    :param personid: int, individual PRIMARY KEY of input person. Defaults is max person id + 1, or insert it individually.
    :return: None. Only print report about completed work.
    """
    try:
        with sqlite3.connect(db_name) as conn:
            cur = conn.cursor()
            cur.execute(f"""
            INSERT INTO {table_name}(personid, first_name, last_name, address, job, age)
            VALUES('{personid}', '{first_name}', '{last_name}', '{address}',
            '{job}', '{age}');
            """)
        print(f'Insert of personid №{personid} successfully created')
    except Exception as err:
        print(f'Something wrong```! error is: \n{err}')
        if str(err).find('personid'):
            with sqlite3.connect(db_name) as conn:
                cur = conn.cursor()
                cur.execute(f"""
                SELECT MAX(personid) from {table_name}
                """)
    return


def db_data_generator(counter: int, db_name='db-new.db', table_name='persons') -> None:
    """
    Need for generate data into our DB
    :param counter: int, how many records to generate
    :param db_name: str, default - our DB
    :param table_name: str, default - table `persons`
    :return: None. Only print report about completed work, and how many lines generated.
    """
    start_generation = db_linecouter()  # need for counting of lines of data witch generated and inserted into the DB
    try:
        for _ in range(counter):
            db_insert(person.first_name(), person.last_name(),
                      f'{address.address()}, {address.city()}', f'{person.occupation()} in `{finance.company()}`',
                      person.age(minimum=18), personid=db_max_personid()+1, db_name=db_name, table_name=table_name)
        print(f'Generationg was finishd. There are inserted {db_linecouter() -start_generation} peoples')
    except Exception as err:
        print(f'Something wrong111! error is: \n{err}')


def db_sortperson(db_name='db-new.db', table_name='persons', sortby='personid') -> list or int:
    """
    Need for sorting data from database.
    :param db_name: str, default - our DB
    :param table_name: str, default - table `persons`
    :param sortby: any, param witch we need to sort by. Default is 'personid'
    :return: list, List of tuples with sort data, if error - return -1
    """
    person_list = []
    try:
        with sqlite3.connect(db_name) as conn:
            cur = conn.cursor()
            cur.execute(f"""
            SELECT *
            FROM {table_name} ORDER BY '{sortby}'
            """)
            data = cur.fetchall()
            for row in data:
                person_list.append(row)
        return person_list
    except Exception as err:
        print(f'Something wrong111! error is: \n{err}')
        return -1


def db_personreturn(sortrule: any, sortvalue: any, db_name='db-new.db', table_name='persons') -> list or int:
    """
    Print a tuple of persons that are associated with a given params
    :param sortrule: any, column witch we are returning data.
    :param sortvalue: any, rule witch we are returning data from the column.
    :param db_name: str, default - our DB
    :param table_name: str, default - table `persons`
    :return: list, List of tuples with selected data WHERE ... if error - reutrn -1
    """
    person_list = []
    try:
        with sqlite3.connect(db_name) as conn:
            cur = conn.cursor()
            cur.execute(f"""
            SELECT *
            FROM {table_name} WHERE {sortrule} = '{sortvalue}';
            """)
            data = cur.fetchall()
            for row in data:
                person_list.append(row)
        return person_list
    except Exception as err:
        print(f'Something wrong111! error is: \n{err}')
        return -1


db_creator('db-new.db', 'persons')

db_insert('Alex', 'Pro', 'Odessa', 'an Officer', 30)

#db_data_generator(10000)

print(db_sortperson(sortby='age'))

print(db_personreturn('age', 18))

print(db_personreturn('address', 'Odessa'))

print('There is', db_max_personid(), 'the max of peronid value')
print('There are', db_linecouter(), 'lines of data int our DB, person table')