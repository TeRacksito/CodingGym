"""
A module to handle database operations.
"""

import json
import traceback
from typing import Optional

import mysql.connector

from plib.terminal import error
from plib.utils.custom_exceptions import BranchWarning
from plib.utils.general import getCurrentBranch


class Database:
    """
    A class to handle database operations.

    Raises
    ------
    `KeyError` or `FileNotFoundError`
        If the file TOKEN.json does not exist or does not contain the database credentials.
    """
    def __init__(self) -> None:
        try:
            with open("TOKEN.json", encoding="utf-8") as file:
                data = json.load(file)
                host = data["db_host"]
                user = data["db_user"]
                password = data["db_pass"]
                database = data["db_name"]

            self.mainDb = mysql.connector.connect(
                host= host,
                user= user,
                password= password,
                database= database
            )
        except (KeyError, FileNotFoundError) as e:
            error(e, traceback.format_exc(), "Database credentials not found",
                  "Please make sure that the file TOKEN.json exists and contains the database credentials.", level="WARNING")
            raise

    def _check_table(self, table: str):
        """
        Checks if a table exists in the database.

        Parameters
        ----------
        table : `str`
            Table name to check.
        
        Returns
        -------
        `bool`
            True or False depending on whether the table exists or not.
        """
        cursor = self.mainDb.cursor()

        cursor.execute("SELECT table_name FROM information_schema.tables")

        tabes = cursor.fetchall()

        for table_name in tabes:
            if table in table_name:
                return True

        return False

    def insert(self, table: str, names: list, values: list, retrieve_id: bool = False):
        """
        Inserts a register into a table.
        
        Parameters
        ----------
        table : `str`
            Which table to insert the register into.
        names : `list`
            Field names. ej: ["name1", "age1", "name2", "age2"]
        values : `list`
            Field values. ej: ["John", 20, "Mary", 21]
        
        Raises
        ------
        `NameError`
            If the table does not exist.
        `BranchWarning`
            If the current branch is not `main`.
            This is to prevent accidental changes to the database.
        """
        if not self._check_table(table=table):
            raise NameError(f"Table {table} does not exist.")

        cursor = self.mainDb.cursor()

        names_str = "("
        for name_index, name in enumerate(names):
            if name_index > 0:
                names_str += ", "
            names_str += name
        names_str += ")"

        values_str = "("
        for value_index, _ in enumerate(values):
            if value_index > 0:
                values_str += ", "
            values_str += r"%s"
        values_str += ")"
        print(values_str)

        sql = f"INSERT INTO {table} {names_str} VALUES {values_str}"
        print(sql)

        cursor.execute(sql, values)

        self.mainDb.commit()

        print(cursor.rowcount, "record(s) affected")
        if retrieve_id:
            return cursor.lastrowid

    def select(self, table: str, conditions: Optional[dict] = None):
        """
        Selects registers from a table.        
        
        Parameters
        ----------
        table : `str`
            Which table to select the register or registers from.
        conditions : `dict, optional`
            A dictionary as {Field_name: Field_values} to filter among registers.
            If undefined, then all registers from table would be returned, by default `None`
        
        Returns
        -------
        `list[(tuple,)]`
            list of registers as tuples. Could be empty.
            Could be other type format depending on the table.
        
        Raises
        ------
        `NameError`
            If the table does not exist.
        """
        if not self._check_table(table=table):
            raise NameError(f"Table {table} does not exist.")
        cursor = self.mainDb.cursor()

        sql = f"SELECT * FROM {table}"

        if conditions:
            sql += " WHERE "

            for condition_index, condition in enumerate(conditions):
                if condition_index > 0:
                    sql += " AND "
                sql += f"{condition} = \"{conditions[condition]}\""

        cursor.execute(sql)

        payload = cursor.fetchall()

        return payload

    def update(self, table: str, key_name: str, key_value: str, value_name: str, value_value: int):
        """
        Updates a field value from a register.

        Parameters
        ----------
        table : `str`
            Which table to update the register from.
        key_name : `str`
            Field name to filter the register.
        key_value : `str`
            Field value to filter the register.
        value_name : `str`
            Field name to update.
        value_value : `int`
            Field value to update.
        
        Raises
        ------
        `NameError`
            If the table does not exist.
        `BranchWarning`
            If the current branch is not `main`.
            This is to prevent accidental changes to the database.
        """
        if not self._check_table(table=table):
            raise NameError(f"Table {table} does not exist.")

        if getCurrentBranch() != "main":
            try:
                raise BranchWarning(branch=getCurrentBranch())
            except BranchWarning as e:
                error(e, traceback.format_exc(), "Not on main branch",
                      "Please switch to the main branch to update the database.", level="WARNING")
                raise

        cursor = self.mainDb.cursor()

        sql = f"UPDATE {table} SET {value_name} = {value_value} WHERE {key_name} = \"{key_value}\""

        print(sql)

        cursor.execute(sql)

        self.mainDb.commit()

        print(cursor.rowcount, "record(s) affected")

    def delete(self, table: str, conditions: dict):
        """
        Deletes a register or registers from a table.

        Warning: Conditions can't be empty.
        Warning: If conditions is not empty,
        then all registers that match the conditions would be deleted.
        
        Parameters
        ----------
        table : `str`
            Which table to delete the register or registers from.
        conditions : `dict`
            A dictionary as {Field_name: Field_values} to filter among registers.
        
        Raises
        ------
        `NameError`
            If the table does not exist.
        `BranchWarning`
            If the current branch is not `main`.
            This is to prevent accidental changes to the database.
        `ValueError`
            If conditions is empty.
        """
        if not self._check_table(table=table):
            raise NameError(f"Table {table} does not exist.")

        if getCurrentBranch() != "main":
            try:
                raise BranchWarning(branch=getCurrentBranch())
            except BranchWarning as e:
                error(e, traceback.format_exc(), "Not on main branch",
                      "Please switch to the main branch to update the database.", level="WARNING")
                raise

        if not conditions:
            raise ValueError("Conditions can't be empty.")

        cursor = self.mainDb.cursor()

        sql = f"DELETE FROM {table} WHERE "

        for condition_index, condition in enumerate(conditions):
            if condition_index > 0:
                sql += " AND "
            sql += f"{condition} = \"{conditions[condition]}\""

        print(sql)

        cursor.execute(sql)

        self.mainDb.commit()

        print(cursor.rowcount, "record(s) affected")
