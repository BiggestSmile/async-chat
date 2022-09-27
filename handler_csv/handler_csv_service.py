import os
import re
import pandas as pd


class HandlerCsvService:
    def get_files_list(self, path=r'.\\data\\input\\'):
        for file in os.listdir(path):
            if os.path.isfile(os.path.join(path, file)):
                yield path + file

    def _parse_line(self, line):
        """
        Do a regex search against all defined regexes and
        return the key and match result of the first matching regex

        """

        rx_dict = {
            'os_prod': re.compile(r'Изготовитель ОС:(?P<os_prod>.*)\n'),
            'os_name': re.compile(r'Название ОС:(?P<os_name>.*)\n'),
            'os_code': re.compile(r'Код продукта:(?P<os_code>.*)\n'),
            'os_type': re.compile(r'Тип системы:(?P<os_type>.*)\n'),
        }

        for key, rx in rx_dict.items():
            match = rx.search(line)
            if match:
                return key, match
        # if there are no matches
        return None, None

    def parse_file(self, filepath):
        """
        Parse text at given filepath

        Parameters
        ----------
        filepath : str
            Filepath for file_object to be parsed

        Returns
        -------
        data : pd.DataFrame
            Parsed data

        """

        data = []  # create an empty list to collect the data
        # open the file and read through it line by line
        with open(filepath, 'r') as file_object:
            line = file_object.readline()
            while line:
                # at each line check for a match with a regex
                key, match = self._parse_line(line)

                if key == 'os_prod':
                    os_prod = match.group('os_prod')
                if key == 'os_name':
                    os_name = match.group('os_name')
                if key == 'os_code':
                    os_code = match.group('os_code')
                if key == 'os_type':
                    school = match.group('os_type')


                # extract school name
                if key == 'school':
                    school = match.group('school')

                # extract grade
                if key == 'grade':
                    grade = match.group('grade')
                    grade = int(grade)

                # identify a table header
                if key == 'name_score':
                    # extract type of table, i.e., Name or Score
                    value_type = match.group('name_score')
                    line = file_object.readline()
                    # read each line of the table until a blank line
                    while line.strip():
                        # extract number and value
                        number, value = line.strip().split(',')
                        value = value.strip()
                        # create a dictionary containing this row of data
                        row = {
                            'School': school,
                            'Grade': grade,
                            'Student number': number,
                            value_type: value
                        }
                        # append the dictionary to the data list
                        data.append(row)
                        line = file_object.readline()

                line = file_object.readline()

            # create a pandas DataFrame from the list of dicts
            data = pd.DataFrame(data)
            # set the School, Grade, and Student number as the index
            data.set_index(['School', 'Grade', 'Student number'], inplace=True)
            # consolidate df to remove nans
            data = data.groupby(level=data.index.names).first()
            # upgrade Score from float to integer
            data = data.apply(pd.to_numeric, errors='ignore')
        return data

    def get_data(self):
        for file_path in service_csv.get_files_list():
            print(file_path)
            with open(file_path) as file:
                file_contents = file.read()
                print(file_contents)

    def write_to_csv(self):
        pass

    def run_service(self):
        self.get_data()


if __name__ == '__main__':
    service_csv = HandlerCsvService()
    service_csv.run_service()


