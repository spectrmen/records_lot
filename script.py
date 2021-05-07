# -*- coding: UTF-8 -*-
import sys
from datetime import datetime
import traceback
import time
from colorama import Fore, init
import httplib2
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from psycopg2 import connect, sql

init(convert=True,autoreset=True)
class GoogleSheets:
    def __init__(self):
        #предел количества записей в один рабочий лист
        self.COLUMNS = ['Дата', 'Время', 'Номенклатура', 'Характеристика', 'Штрихкод', 'ЛОТ']
        #адрес нужен, чтобы давать права на редакирование документов
        self.MAIL = "spectrmen123@gmail.com"
        #лимит числа записей, на котором происходит запись в базу
        self.LIMIT = 5
        self.FLAG = 'ru'
        #все записи
        self.old_records = []
        #записи которые пойдут в базу
        self.records = []
        #штрих коды
        self.data_codes = []
        #Вся информация по штрихкоду храниться отдельно из-за конфликта типов
        self.data = []
        #для перевода на анлглийский
        self._eng_chars = u"~!@#$%^&qwertyuiop[]asdfghjkl;'zxcvbnm,./QWERTYUIOP{}ASDFGHJKL:\"|ZXCVBNM<>?"
        self._rus_chars = u"ё!\"№;%:?йцукенгшщзхъфывапролджэячсмитьбю.ЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭ/ЯЧСМИТЬБЮ,"
        self._trans_table = dict(zip(self._rus_chars, self._eng_chars))
        #курсор для работы с PostgreSQL
        self.cursor = ''

    def check_lang(self,s):
        if self.FLAG == 'en':
            return u''.join([self._trans_table.get(c, c) for c in s])
        elif self.FLAG == 'ru':
            return s

    def authorize(self):
        # Имя файла с закрытым ключом, вы должны подставить свое
        CREDENTIALS_FILE = ''
        # Читаем ключи из файла
        credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
        self.client = gspread.authorize(credentials)
        self.connection = connect(user='admin', password='admin', host='65.21.106.30',
                             port='5432', database='script_db')
        self.cursor = self.connection.cursor()
    def check_record(self, record):
        key_of_names = {'Дата':'date',
                         'Время': 'time',
                         'Характеристика': 'har',
                         'Номенклатура':'nomen',
                         'ЛОТ':'lot',
                         'Штрихкод': 'code'}
        temp = []
        for h in self.COLUMNS:
            temp.append(record[key_of_names[h]])
        if temp[4:] in self.old_records:
            print(f'{Fore.YELLOW}такая запись уже существует')
            self.spreadsheet.values_append('Повторы', {'valueInputOption': 'RAW'}, {'values': [temp]})
            print(f'{Fore.GREEN}создана запись:\n Повторы %s \n' % temp)
            #запись в базу postgres
            insert = sql.SQL('INSERT INTO repetitions (date_ap, time_ap, nomen, feature, barcode, lot) VALUES ({});').format(
                sql.SQL(',').join(map(sql.Literal, temp))
            )
            self.cursor.execute(insert)
            self.connection.commit()
            print('создана запись POSTGRES \"repetitions\"')
            return 0
        else:
            self.records.append(temp)
            self.old_records.append(temp[4:])
            #запись в postgres
            insert = sql.SQL('INSERT INTO records (date_ap, time_p, nomen, feature, barcode, lot) VALUES ({});').format(
                sql.SQL(',').join(map(sql.Literal, temp))
            )
            self.cursor.execute(insert)
            self.connection.commit()
            print('создана запись POSTGRES \"records\"')
            return 1

    def record_to_sheet(self):
        if len(self.records)>0:
            self.spreadsheet.values_append(self.worksheet.title, {'valueInputOption': 'RAW'}, {'values': self.records})
            print(f'{Fore.GREEN}создана запись:\n {self.worksheet.title} %s \n' % self.records)
            self.records.clear()

    def check_lot(self):
        lot = input('введите лот ')
        if lot == '':
            print(f'{Fore.RED}лот введен не корректно', end='\n\n')
            check_lot()
        else:
            return self.check_lang(lot)

    def input_lang(self):
        lang = input('введите язык ввода (ru,en): ')
        if lang not in ['ru', 'en']:
            print(f'{Fore.RED}язык введен некорректно', end='\n\n')
            self.input_lang()
        else:
            return lang

    def load_all_data(self):
        self.cursor.execute('SELECT nomen, feature, barcode FROM library')
        self.data = pd.DataFrame(self.cursor.fetchall(),columns=['Номенклатура','Характеристика','Штрихкод'])
        if self.data:
            self.data.fillna('', inplace=True)
            self.data_codes = [str(code) for code in self.data['Штрихкод'] if code]
                                                      #код заменить
            self.spreadsheet = self.client.open_by_key('1MMp-qDaUE3JzIye4xSNkFq0lEZJm5wt2zN7MUNHtYkM')
            print(self.spreadsheet)
            self.worksheet = self.spreadsheet.get_worksheet(0)

            self.cursor.execute('SELECT barcode, lot FROM records')
            self.old_records.extend([[a[0], a[1]] for a in self.cursor.fetchall()])
        else:
             raise Exception(f'''{Fore.RED}СРОЧНО! Обратитесь к Беликову Евгению.
                    Обнаружена ПРОБЛЕМА с обменом базы данных!
                    Дальнейшая работа НЕВОЗМОЖНА!''')
    def run(self):
        start_time = time.time()
        self.authorize()
        print("--- %s seconds authorize ---" % (time.time() - start_time))
        print("соединение postgres \"script_db\" успешно ")
        start_time = time.time()
        # загрузка данных
        self.load_all_data()
        print("--- %s seconds get data ---" % (time.time() - start_time))
        #счетчик записей
        num_records = 0
        #определение языка ввода
        self.FLAG = self.input_lang()
        try:
            while True:
                    record = {}
                    #количество записей, после которых происходит запись в базу
                    if num_records >= self.LIMIT:
                        self.record_to_sheet()
                        num_records = 0
                    code = input('введите штрих-код ')
                    if code not in self.data_codes:
                        print(f'{Fore.RED}такогого штрих кода нет', end='\n\n')
                        continue
                    record['code'] = code
                    record['lot'] = self.check_lot()
                    record['date'] = datetime.now().strftime("%Y.%m.%d")
                    record['time'] = datetime.now().strftime("%H:%M:%S")
                    record['nomen'], record['har'] = self.data[self.data['Штрихкод']==code].loc[:,['Номенклатура','Характеристика']].values.tolist()[0]
                    num_records += self.check_record(record)
        except KeyboardInterrupt:
            print('до свидания')
            #запись в базу при закрытии программы
            self.record_to_sheet()
            self.cursor.close()
            self.connection.close()
            sys.exit(0)

if __name__ == '__main__':
    try:
        GS = GoogleSheets()
        GS.run()
    except:
        traceback.print_exception(*sys.exc_info())
        print("Program ended, press Enter to quit.")
        _ = sys.stdin.read(1)
