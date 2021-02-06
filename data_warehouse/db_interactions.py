from sqlalchemy import create_engine
import psycopg2
import pandas as pd


def write_record(name, details, engine):
    """write_record: writing new record to database

    Args:
        name (str): Name of the record to insert into database
        details (str): String details of the record to insert into database
        engine (obj): Engine object using the create_record function
    """

    engine.execute(
        "INSERT INTO records (name,details) VALUES ('%s','%s')" % (name, details))


def read_record(field, name, engine):
    """read_record: Selecting a record from the database

    Args:
        field (str): SELECT reference item
        name (str): Name to select from the database
        engine (obj): Engine object using the create_record function

    Returns:
        result: Result from the database query
    """

    result = engine.execute(
        "SELECT %s FROM records WHERE name = '%s'" % (field, name))
    return result.first()[0]


def update_record(field, name, new_value, engine):
    """update_record: Update a record from the database

    Args:
        field (str): Field to be updated
        name (str): Name to update based on
        new_value (str): New vaule to be added to the database
        engine (obj): Engine object using the create_record function
    """

    engine.execute("UPDATE records SET %s = '%s' WHERE name = '%s'" %
                   (field, new_value, name))


def write_dataset(name, dataset, engine):
    """write_dataset: Write the dataset to database

    Args:
        name (str): Name of the dataset to be written
        dataset (obj): Pandas object representing the dataset
        engine (obj): Engine object using the create_record function
    """

    dataset.to_sql('%s' % (name), engine, index=False,
                   if_exists='replace', chunksize=1000)


def read_dataset(name, engine):
    """read_dataset: Read the dataset based on provided name

    Args:
        name (str): Name of the dataset to read
        engine (obj): Engine object using the create_record function

    Returns:
        dataset: Dataset object
    """

    try:
        dataset = pd.read_sql_table(name, engine)
    except:
        dataset = pd.DataFrame([])
    return dataset


def list_datasets(engine):
    """list_datasets: Return a list of datasets

    Args:
        engine (obj): Engine object using the create_record function

    Returns:
        datasets: Returns all datasets
    """

    datasets = engine.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
    return datasets.fetchall()
