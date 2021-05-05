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

init(convert=True,autoreset=True)
class GoogleSheets:
    def __init__(self):
        #предел количества записей в один рабочий лист
        self.COLUMNS = ['Дата', 'Время', 'Номенклатура', 'Характеристика', 'Штрихкод', 'ЛОТ']
        #адрес нужен, чтобы давать права на редакирование документов
        self.MAIL = "spectrmen123@gmail.com"
        #лимит числа записей, на котором происходит запись в базу
        self.LIMIT_ROWS = 30
        self.LIMIT = 1
        self.FLAG = 'ru'
        self.old_records = []
        #для перевода на анлглийский
        self._eng_chars = u"~!@#$%^&qwertyuiop[]asdfghjkl;'zxcvbnm,./QWERTYUIOP{}ASDFGHJKL:\"|ZXCVBNM<>?"
        self._rus_chars = u"ё!\"№;%:?йцукенгшщзхъфывапролджэячсмитьбю.ЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭ/ЯЧСМИТЬБЮ,"
        self._trans_table = dict(zip(self._rus_chars, self._eng_chars))
        self.num_of_spreadsheets = 0

    def check_lang(self,s):
        if self.FLAG == 'en':
            return u''.join([self._trans_table.get(c, c) for c in s])
        elif self.FLAG == 'ru':
            return s

    def authorize(self):
        # Имя файла с закрытым ключом, вы должны подставить свое
        #CREDENTIALS_FILE = 'tough-progress-290810-931e9d0e3542.json'
        CREDENTIALS_FILE = 'mindful-oath-289809-f6e01a52b04d.json'
        # Читаем ключи из файла
        credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
        self.client = gspread.authorize(credentials)

    def create_worksheet(self):
        self.spreadsheet = self.client.create('Таблица Лотов {}'.format(datetime.now().strftime("%Y.%m.%d")))
        self.spreadsheet.share(self.MAIL, perm_type='user', role='writer')
        self.worksheet = self.spreadsheet.sheet1
        self.worksheet.resize(rows="20000", cols="20")
        self.worksheet.update_title("Номенклатура и Лоты {}".format(datetime.now().strftime("%Y.%m.%d")))
        self.worksheet.update([self.COLUMNS])
        repits = self.spreadsheet.add_worksheet(title="Повторы {}".format(datetime.now().strftime("%Y.%m.%d")), rows="20000", cols="20")
        repits.update([self.COLUMNS])
        self.num_of_spreadsheets += 1
        print(f'{Fore.YELLOW}превышено допустимое количество записей создан новый лист \"{self.spreadsheet.title}\"!')

    def record_to_sheet(self,records):
        if len(self.old_records)/self.num_of_spreadsheets > self.LIMIT_ROWS:
            self.create_worksheet()
        key_of_names = {'Дата':'date',
                        'Время': 'time',
                        'Характеристика': 'har',
                        'Номенклатура':'nomen',
                        'ЛОТ':'lot',
                        'Штрихкод': 'code'}
        put_values = []
        for v in records:
            temp = []
            for h in self.COLUMNS:
                temp.append(v[key_of_names[h]])
            if temp[4:] in self.old_records:
                print(f'{Fore.YELLOW}такая запись уже существует')
                self.spreadsheet.values_append('Повторы', {'valueInputOption': 'RAW'}, {'values': [temp]})
                print(f'{Fore.GREEN}создана запись:\n Повторы %s \n' % temp)
            else:
                put_values.append(temp)
                self.old_records.append(temp[4:])
        if len(put_values)>0:
            self.spreadsheet.values_append(self.worksheet.title, {'valueInputOption': 'RAW'}, {'values': put_values})
            print(f'{Fore.GREEN}создана запись:\n {self.worksheet.title} %s \n' % put_values)

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
        for spreadsheet in self.client.openall():
            self.num_of_spreadsheets += 1
            self.spreadsheet = spreadsheet
            print(self.spreadsheet)
            self.worksheet = self.spreadsheet.get_worksheet(0)
            self.old_records.extend([[a[4], a[5]] for a in self.worksheet.get_all_values()[1:]])

    def run(self):
        start_time = time.time()
        self.authorize()
        print("--- %s seconds authorize ---" % (time.time() - start_time))
        start_time = time.time()
        # cтарые записи для проверки
        self.load_all_data()
        print("--- %s seconds get data ---" % (time.time() - start_time))
        #подзагрузка базы штрихкодов
        start_time = time.time()
        data = pd.read_excel('J&J.xlsx', header=3, dtype={'Штрихкод':str})
        data = data.loc[:,['Номенклатура','Характеристика','Штрихкод']]
        data.fillna('', inplace=True)
        data_codes = [str(code) for code in data['Штрихкод'] if code]
        print("--- %s seconds data_codes ---" % (time.time() - start_time))
        #счетчик записей
        num_records = 0
        records = []
        #определение языка ввода
        self.FLAG = self.input_lang()
        try:
            while True:
                    record = {}
                    #количество записей, после которых происходит запись в базу
                    if num_records >= self.LIMIT:
                        self.record_to_sheet(records)
                        num_records = 0
                        records = []
                    code = input('введите штрих-код ')
                    if code not in data_codes:
                        print(f'{Fore.RED}такогого штрих кода нет', end='\n\n')
                        continue
                    record['code'] = code
                    record['lot'] = self.check_lot()
                    record['date'] = datetime.now().strftime("%Y.%m.%d")
                    record['time'] = datetime.now().strftime("%H:%M:%S")
                    record['nomen'], record['har'] = data[data['Штрихкод']==code].loc[:,['Номенклатура','Характеристика']].values.tolist()[0]
                    records.append(record)
                    num_records +=1
        except KeyboardInterrupt:
            print('до свидания')
            #запись в базу при закрытии программы
            self.record_to_sheet(records, spreadsheet, old_records)
            sys.exit(0)

if __name__ == '__main__':
    try:
        GS = GoogleSheets()
        GS.run()
    except:
        traceback.print_exception(*sys.exc_info())
        print("Program ended, press Enter to quit.")
        _ = sys.stdin.read(1)
