from datetime import datetime
import json
import sqlalchemy
from sqlalchemy import create_engine, text, MetaData, Table, select, inspect
from sqlalchemy.orm import sessionmaker

class SQLManager:
    def __init__(self):
        self.engine = None
        self.Session = None
        self.session = None
        self.metadata = MetaData()
        self.conn = None
        self.cur = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()

    def connect_with_url(self, url):
        #try:
            self.engine = create_engine(url)
            self.Session = sessionmaker(bind=self.engine)
            self.session = self.Session()
            self.metadata.reflect(bind=self.engine)
        #except sqlalchemy.exc.InterfaceError as e:
        #    print("An error occurred while connecting to the database: ", str(e))

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    #def upsert(self, table_name, _dict):
    #    metadata = MetaData(self.engine)
    #    table = Table(table_name, metadata, autoload_with=self.engine)
    #   # Check if the record exists
    #    query = select([table]).where(table.c.id == _dict['id'])
    #    existing_record = self.session.execute(query).fetchone()
    #    if existing_record:
            # Record exists, so update
    #        update_query = table.update().where(table.c.id == _dict['id']).values(**_dict)
    #        self.session.execute(update_query)
    #    else:
    #        # Record does not exist, so insert
    #       insert_query = table.insert().values(**_dict)
    #        self.session.execute(insert_query)
    #    self.session.commit()

    #def delete(self, table_name, _id):
        #delete_stmt = text(f"DELETE FROM {table_name} WHERE id = :id")
        #self.session.execute(delete_stmt, {'id': _id})
        #self.session.commit()

    def get(self, table_name, _id):
        select_stmt = text(f"SELECT * FROM {table_name} WHERE id = :id")
        result = self.session.execute(select_stmt, {'id': _id})
        return result.fetchone()

    def get_all(self, table_name):
        select_all_stmt = text(f"SELECT * FROM {table_name}")
        result = self.session.execute(select_all_stmt)
        return result.fetchall()

    # def run_sql(self, sql):
    #     self.cur.execute(sql)
    #     return self.cur.fetchall()

    def run_sql(self, sql) -> str:
        result = self.session.execute(text(sql))
        columns = result.keys()
        rows = result.fetchall()
        list_of_dicts = [dict(zip(columns, row)) for row in rows]

        json_result = json.dumps(list_of_dicts, indent=4, default=self.datetime_handler)
        return json_result

    def datetime_handler(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)

    def get_table_definition(self, table_name):
        try:
            # Reflect the database schema
            self.metadata.reflect(bind=self.engine)

            # Attempt to retrieve the table from the metadata
            table = self.metadata.tables[table_name]

            # Start building the CREATE TABLE statement
            create_table_stmt = f"CREATE TABLE {table_name} (\n"

            # Add column definitions to the statement
            for column in table.columns:
                create_table_stmt += f"    {column.name} {column.type},\n"

            # Remove the trailing comma and newline, then close the statement
            create_table_stmt = create_table_stmt.rstrip(",\n") + "\n);"

            return create_table_stmt
        except KeyError:
            # Log a warning and return None if the table is not found
            print(f"Warning: Table {table_name} not found in the database. Skipping.")
            return None

    def reflect_tables(self):
        self.metadata = MetaData()

        # Reflect tables for each schema
        for schema in ['dim', 'fact', 'dbo']:
            self.metadata.reflect(bind=self.engine, schema=schema)

    def get_all_table_names(self):
        table_names = []
        for schema in ['dim', 'fact', 'dbo']:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema=schema)
            # Prefix table name with schema
            table_names.extend([f"{schema}.{table}" for table in tables])
        return table_names

    def get_table_definitions_for_prompt(self):
        self.reflect_tables()
        table_names = self.get_all_table_names()
        definitions = []
        for table_name in table_names:
            try:
                # Access table with schema prefix
                table = self.metadata.tables[table_name]
                columns = ["{} {}".format(column.name, column.type) for column in table.columns]
                table_definition = "CREATE TABLE {} (\n  {});".format(table_name, ',\n  '.join(columns))
                definitions.append(table_definition)
            except KeyError:
                print("Error accessing " + table_name)
                continue
        return "\n\n".join(definitions)
    
    def get_table_definition_map_for_embeddings(self):
        table_names = self.get_all_table_names()
        definitions = {}
        for table_name in table_names:
            table_def = self.get_table_definition(table_name)
            if table_def is not None:
                definitions[table_name] = table_def
        return definitions

    def get_related_tables(self, table_list, n=2):
        """
        Get tables that have foreign keys referencing the given table and tables referenced by the given table in SQL Server.
        """

        related_tables_dict = {}

        for table in table_list:
            # Query to fetch tables that have foreign keys referencing the given table
            self.cur.execute(
                """
                SELECT DISTINCT 
                    OBJECT_NAME(fk.referenced_object_id) AS table_name
                FROM 
                    sys.foreign_keys AS fk
                    JOIN sys.tables AS t ON fk.parent_object_id = t.object_id
                WHERE 
                    OBJECT_NAME(fk.parent_object_id) = %s;
                """,
                (table,)
            )

            related_tables = [row[0] for row in self.cur.fetchall()]

            # Query to fetch tables that the given table references
            self.cur.execute(
                """
                SELECT DISTINCT 
                    OBJECT_NAME(fk.parent_object_id) AS referenced_table_name
                FROM 
                    sys.foreign_keys AS fk
                    JOIN sys.tables AS t ON fk.referenced_object_id = t.object_id
                WHERE 
                    OBJECT_NAME(fk.referenced_object_id) = %s;
                """,
                (table,)
            )

            related_tables += [row[0] for row in self.cur.fetchall()]

            related_tables_dict[table] = related_tables

        # Convert dict to list and remove duplicates
        related_tables_list = []
        for _, related_tables in related_tables_dict.items():
            related_tables_list += related_tables

        related_tables_list = list(set(related_tables_list))

        return related_tables_list
