import logging
import os

from crypto_screener.constants import SEPARATOR


class CreateViewsStep:

    def __init__(self, config: dict, crypto_screener_db_conn):
        self.crypto_screener_db_con = crypto_screener_db_conn
        self.view_folder = config["steps"]["createViewsStep"]["viewFolder"]

    def process(self) -> None:
        logging.info(SEPARATOR)
        logging.info("Start create views step")
        logging.info(SEPARATOR)

        view_files = os.listdir(self.view_folder)

        for view_file in view_files:
            try:
                logging.info("Create view from file: {}".format(view_file))
                create_view_statement = self.__read_file("{}/{}".format(self.view_folder, view_file))
                cursor_obj = self.crypto_screener_db_con.cursor()
                cursor_obj.executescript(create_view_statement)
            except:
                logging.exception("Problem with creating view from file: {}".format(view_file))

        logging.info(SEPARATOR)
        logging.info("Finished create views step")
        logging.info(SEPARATOR)

    @staticmethod
    def __read_file(path: str) -> str:
        file = open(path, mode="r")
        result = file.read()
        file.close()
        return result
