#!/usr/bin/env python3
# coding=UTF-8
import pymssql
import pandas as pd
import re
import sys
import numpy as np
from itertools import chain

try:
    # load db_config from file
    config_df = pd.read_csv('config\\db_config.csv', index_col=0)
    db_config = config_df.loc['config'].dropna().to_dict()
    db_address = config_df.loc['address'].dropna().to_dict()
except FileNotFoundError:
    print('db_config.csv is not found in config!')
    sys.exit(1)


# A standard python dictionary class
class Dict:
    def __init__(self):
        self._dict = {}

    def __iter__(self):
        for key, value in self._dict.items():
            yield key, value

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        self._dict[key] = value

    def __contains__(self, item):
        return item in self._dict

    def __len__(self):
        return len(self._dict)

    def keys(self):
        for key in self._dict.keys():
            yield key

    def values(self):
        for value in self._dict.values():
            yield value

    def get(self, key, default=0):
        return self._dict.get(key, default)

    def update(self, dictionary):
        self._dict.update(dictionary)

    @property
    def dict(self):
        return self._dict


class Connection:
    # class variable

    def __init__(self):
        self._conn = pymssql.connect(**db_config)

    @staticmethod
    def _sql_string(value):
        """replace the double quote of a string if it's a string"""
        if isinstance(value, str):
            # replace the begin end end double quote to single quote
            return re.sub(r'^"|"$', "'", repr(value.replace("'", "''")))
        else:
            return repr(value)

    def select_sql(self, selector='', addr='', where=None, addition='', replace_sql='', get_df=False, df_index=None):
        """
        select function
        :param selector: the select field
        :param addr: the table address
        :param where: condition
        :param addition: additional sql query
        :param replace_sql: replace the whole sql query
        :param get_df: bool to return a df or a list
        :param df_index: index of the returned DataFrame
        :return: return a DataFrame or value of the selector
        """
        # if replace_sql is True
        if replace_sql:
            sql = replace_sql
        else:
            sql = f'SELECT {selector} FROM {addr}'
            if where:
                where = {col: self._sql_string(value) for col, value in where.items()}
                for i, (col, value) in enumerate(where.items()):
                    sql += f' {"WHERE" if i == 0 else "AND"} {col} = {value}'
        sql += addition
        cursor = self._conn.cursor(as_dict=get_df)
        cursor.execute(sql)
        rs = cursor.fetchall()
        return pd.DataFrame(rs, index=df_index) if get_df else rs[0][0] if len(rs) > 0 else 0

    def insert_sql(self, addr, insert):
        """
        Insert insert_dictionary (key=field, value=value)
        :param addr: the table address
        :param insert: dictionary (key=field, value=value)
        """
        insert_list = [
            [col, self._sql_string(value)] for col, value in insert.items()
        ]
        # join the transposed list
        cols, values = map(', '.join, list(zip(*insert_list)))
        values = values.replace('\\n', "' + CHAR(10) + '")
        sql = f'INSERT INTO {addr} ({cols}) VALUES ({values})'
        sql = sql.replace("\\", "")
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            self._conn.commit()
            # Uncomment to show what is inserted
            # print('{} is added in {} at {}.'.format(values, cols, addr))
        except pymssql.IntegrityError:
            print(f'An entry has already existed in {addr}.')
            print('Please check if all the fas names are correctly filled, e.g. CC Group has a different fas_name')
            print('Or the same theme has duplicate sv and cc.')
            # sys.exit(1)

    def update_sql(self, addr, col, col_value, condition_col, condition_col_value):
        """
        Update sql query
        :param addr: the table address
        :param col: field
        :param col_value: field value
        :param condition_col: check field
        :param condition_col_value: check field value
        :return:
        """
        sql = (
            f'UPDATE {addr} SET {col} = {self._sql_string(col_value)} '
            f'WHERE {condition_col} = {self._sql_string(condition_col_value)}'
        )
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            self._conn.commit()
            print(f'{col_value} is updated in {col} at {addr}.')
        except pymssql.IntegrityError:
            print(f'Error in updating {addr}.')
            return 1

    def get_latest_id(self, addr, col):
        """get latest id in a table"""
        cursor = self._conn.cursor()
        sql = f'SELECT MAX({col}) FROM {addr}'
        cursor.execute(sql)
        rs = cursor.fetchall()
        if len(rs) > 0:
            result = rs[0][0]
            return result
        else:
            return 0


class CommonDataModel(Dict):
    """CommonDataModel section"""
    def __init__(self, name):
        Dict.__init__(self)
        self.name = name

    def __str__(self):
        """use with print and show the basic information and child"""
        str_list = [f'In {self.name}, there is/are {", ".join(self.keys())}.']
        for cdm_code, cdm in self:
            str_list.append(f'--In {cdm_code}, there is/are {", ".join(cdm.keys())}.')
        return '\n'.join(str_list)

    def all_ids(self, include_child=False):
        """iterate over all id"""
        for cdm in self.values():
            if not include_child:
                yield cdm.id
            else:
                for cdm_child in cdm.values():
                    yield cdm.id, cdm_child.id

    def all_ccg(self):
        """iterate over all ccg_id"""
        for cdm in self.values():
            for cdm_child in cdm.values():
                yield cdm_child.ccg_id

    def update_cdm(self, cdm_code, **kwargs):
        """Update after inserting into DB and getting the id back"""
        for attr_field, attr_value in kwargs.items():
            self[cdm_code].set_attr(attr_field, attr_value)

    def update_cdm_child(self, cdm_code, cdm_child_code, **kwargs):
        """Update after inserting into DB and getting the id back"""
        for attr_field, attr_value in kwargs.items():
            self[cdm_code][cdm_child_code].set_attr(attr_field, attr_value)

    def get_id_by_desc(self, desc, *args, **kwargs):
        """for use in parsing fas data"""
        results_list = []
        for cdm in self.values():
            for cdm_child_code, cdm_child in cdm:
                if cdm_child.desc.lower() == desc.lower() or cdm_child.alt_desc.lower() == desc.lower():
                    if all(cdm_child.get_attr(key) == value for key, value in kwargs.items()):
                        result_dict = {'code': cdm_child_code, 'id': cdm.id, 'child_id': cdm_child.id}
                        for attr in args:
                            result_dict[attr] = cdm_child.get_attr(attr)
                        results_list.append(result_dict)
        if results_list:
            return results_list
        else:
            print('Something might went wrong!')
            return 0


class CDM:
    """CDM basic unit, e.g. CC/SV"""
    def __init__(self, desc, desc_tc, alt_desc, alt_desc_tc, **kwargs):
        self.id = 0
        self.desc = desc
        self.desc_tc = desc_tc
        self.alt_desc = alt_desc
        self.alt_desc_tc = alt_desc_tc
        self.__dict__.update(kwargs)

    def get_attr(self, key, default=0):
        """for getting attribute"""
        return getattr(self, key, default)

    def set_attr(self, key, value):
        """for setting attribute"""
        key = key.replace(' ', '').lower()
        setattr(self, key, value)

    def get_tb_desc(self, tc=False):
        """for getting description for CC/CV/SP/SV_TB, replace desc if alt exists"""
        if tc:
            return self.alt_desc_tc if self.alt_desc_tc else self.desc_tc
        else:
            return self.alt_desc if self.alt_desc else self.desc


class CDMGroup(Dict, CDM):
    """CDM basic unit when with child, e.g. CV, SP"""
    def __init__(self, desc, desc_tc, alt_desc, alt_desc_tc, **kwargs):
        Dict.__init__(self)
        CDM.__init__(self, desc, desc_tc, alt_desc, alt_desc_tc)
        self.__dict__.update(kwargs)


class Table(Dict):
    """
    Table object is used for storing CSV related and Table related information
    """
    format_dict = {
        'number': '### ### ##0;-### ### ##0;-##0',
        'number_dot': '### ### ##0.0;-### ### ##0.0;-##0.0',
        'number_sign': '+0.0;-0.0;0.0',
        'dollar': '#,##0_ ',
        'dollar_dot': '#,##0.0_ '
    }

    def __init__(self):
        Dict.__init__(self)
        self.config_df = None
        self.cdm_df = None
        self.code = ''
        self.title = ''
        self.title_tc = ''
        self.fn = ''
        self.fn_tc = ''
        self.src = ''
        self.src_tc = ''
        self.id = 0
        self.cv_cc = CommonDataModel('CV')
        self.sp_sv = CommonDataModel('SP')
        self.mdt = []

    def load_csv(self, path):
        """load csv and parse the split the first two and the rest into two DataFrames"""
        print('This file will be loaded : ' + path)
        if path.lower().endswith('csv'):
            df = pd.read_csv(path, header=None, dtype=str)
        else:
            df = pd.read_excel(path, header=None, dtype=str)
        # select till the second row
        self.config_df = df.iloc[:2, :].dropna(axis=1, how='all')
        # select from the third row and use the second row as column names
        self.cdm_df = df.rename(columns=df.iloc[2]).iloc[3:, :].dropna(axis=1, how='all').reset_index(drop=True)
        # fillna as undefined, usually SV that cannot be found in FAS Table
        self.cdm_df['FAS field name'] = self.cdm_df['FAS field name'].fillna('undefined')

    def parse_config_df(self):
        """parse the config_df"""
        self.code = self.config_df.iloc[0, 1].zfill(3)
        self.title = f'Table {self.code} : {self.config_df.iloc[0, 2]}'
        self.title_tc = f'表{self.code}：{self.config_df.iloc[0, 3]}'

    def parse_cdm_df(self):
        """parse the cdm_df, expand the period, e.g. YYYY according to years in the same CSV"""
        def expand_period(cv_df):
            def enum_cc(series):
                """for CC Code"""
                series['CC Code'] = str(int(series.name) + 1)
                return series

            def period_yyyy(series, year):
                """eval the content inside YYYY so that YYYY-1 will be evaluated"""
                s = series['CC Description'].replace('YYYY', year)
                series['CC Description'] = re.sub(
                    r'\[(.*?)\]', lambda x: str(eval(x.group(1))), s
                )
                return series

            return pd.concat([
                pd.concat([
                    cc_df.copy().apply(lambda x: period_yyyy(x, year), axis=1)
                    for year in cv_df['CV'][cv_df['CV']['FAS field name'] == 'year']['CC Description']
                ], ignore_index=True).apply(enum_cc, axis=1)
                if cc_fas == 'period' else cc_df
                for cc_fas, cc_df in cv_df['CV'].groupby('FAS field name', sort=False)
            ], ignore_index=True)
        # split the df into different section and assign a key according to its first column
        df_dict = {
            field: field_df.dropna(axis=1, how='all')
            for field, field_df in self.cdm_df.groupby(self.cdm_df.columns[0], sort=False)
        }
        if 'M3M' in df_dict['CV']['Common Data Model Code'].values:
            df_dict['CV'] = expand_period(df_dict['CV'])
        # update the self dictionary
        self.update(df_dict)

    def get_theme_code(self):
        return self.config_df.iloc[1, 1].zfill(3)

    def parse_footnote(self, footnote):
        self.fn = footnote.parse('Notes: ')
        self.fn_tc = footnote.parse('註釋：', tc=True)
        self.src = footnote.parse('Source: ', src=True)
        self.src_tc = footnote.parse('資料來源：', src=True, tc=True)

    def init_cv_cc(self, translator, fas_dict):
        for cv_code, cv_cc_df in self['CV'].groupby('Common Data Model Code', sort=False):
            cv_cc_df = cv_cc_df.dropna(axis='columns', how='all')
            cv_cols = ['Common Data Model Code', 'FAS description']
            # Add columns to group if they are not dropped from dropna(not dropped if an entry exists in a col)
            cv_cols += [
                col for col in ['FAS description Chinese', 'Alternate', 'Alternate Chi'] if col in cv_cc_df]
            for cv_values, cc_df in cv_cc_df.groupby(cv_cols, sort=False):
                cv_dict = {cv_cols[i]: value for i, value in enumerate(cv_values) if value}
                cv_desc = cv_dict['FAS description']
                cv_desc_tc = cv_dict.get('FAS description Chinese', translator.translate(cv_desc))
                if 'Alternate' in cv_dict:
                    cv_alt_desc = cv_dict['Alternate']
                    cv_alt_desc_tc = cv_dict.get('Alternate Chi', translator.translate(cv_alt_desc))
                else:
                    cv_alt_desc = ''
                    cv_alt_desc_tc = ''
                self.cv_cc[cv_code] = CDMGroup(cv_desc, cv_desc_tc, cv_alt_desc, cv_alt_desc_tc)

                for cc_seq, (cc_code, cc_series) in enumerate(cc_df.set_index('CC Code').iterrows(), start=1):
                    cc_series.dropna(inplace=True)
                    cc_fas = cc_series['FAS field name']
                    cc_desc = cc_series['CC Description']
                    ccg = int(cc_series['CC Group']) if 'CC Group' in cc_series else 1
                    parent_cc_code = cc_series['Parent CC Code'] if 'Parent CC Code' in cc_series else ''
                    if 'CC Alternate' in cc_series:
                        cc_alt_desc = cc_series['CC Alternate']
                        cc_alt_desc_tc = cc_series[
                            'CC Alternate Chi'
                        ] if 'CC Alternate Chi' in cc_series else translator.translate(cc_alt_desc)
                    else:
                        cc_alt_desc = ''
                        cc_alt_desc_tc = ''

                    if len(str(cc_code)) == 4 and str(cc_code).isdigit():
                        cc_seq = cc_code
                        cc_desc_tc = cc_desc
                    elif re.match(r'^(\()(ix|iv|v?i{0,3}|x?i{0,3})(\))$', cc_desc):
                        cc_desc_tc = cc_desc
                    else:
                        try:
                            float(cc_desc.replace(',', '').replace(' ', ''))
                            cc_desc_tc = cc_desc
                        except ValueError:
                            if 'CC Description Chinese' in cc_series:
                                cc_desc_tc = cc_series['CC Description Chinese']
                            elif cc_fas == 'period' and cv_code == 'M3M':
                                cc_desc_tc = cc_desc
                            else:
                                cc_desc_tc = translator.translate(cc_desc)

                    verify = cc_desc.lower()
                    cc_footnote = fas_dict['CV'][cc_fas].get(verify, {})
                    self.cv_cc[cv_code][cc_code] = CDM(
                        cc_desc, cc_desc_tc, cc_alt_desc, cc_alt_desc_tc,
                        fas=cc_fas, footnote=cc_footnote, seq=cc_seq, ccg=ccg, parent_cc_code=parent_cc_code
                    )

    def init_sp_sv(self, translator, fas_dict):
        for sp_code, sp_sv_df in self['SV'].groupby('SP Code', sort=False):
            sp_sv_df = sp_sv_df.dropna(axis='columns', how='all')
            sp_cols = [
                'SP Code', 'SP Type', 'Unit', 'Unit description', 'decimal', 'unit multipler']
            # Add columns to group if they are not dropped from dropna(not dropped if an entry exists in a col)
            sp_cols += [
                col for col in [
                    'SP Desc', 'SP Desc Chi', 'SP alt', 'SP alt Chi', 'Unit description Chinese', 'NUMBERFORMAT',
                    'SP Footnote FAS name'
                ] if col in sp_sv_df
            ]
            for sp_values, sv_df in sp_sv_df.groupby(sp_cols, sort=False):
                sp_dict = {sp_cols[i]: value for i, value in enumerate(sp_values) if value}
                if 'SP Desc' in sp_dict:
                    sp_desc = sp_dict['SP Desc']
                    sp_desc_tc = sp_dict.get('SP Desc Chi', translator.translate(sp_desc))
                else:
                    sp_desc = ''
                    sp_desc_tc = ''
                sp_alt_desc = sp_dict.get('SP alt', '')
                sp_alt_desc_tc = sp_dict.get('SP alt Chi', '')
                sp_type = sp_dict['SP Type']
                unit = sp_dict['Unit']
                unit_desc = sp_dict['Unit description']
                unit_desc_tc = sp_dict.get(
                    'Unit description Chinese', translator.translate(unit_desc, is_unit=True))
                dec = int(sp_dict['decimal'])
                multi = int(sp_dict['unit multipler'])
                sep = type(self).format_dict.get(sp_dict['NUMBERFORMAT'], '') if 'NUMBERFORMAT' in sp_dict else ''
                sp_fas = sp_dict.get('SP Footnote FAS name', 0)
                if sp_fas:
                    sp_footnote = fas_dict['SV'][sp_fas].get(sp_desc.lower(), {})
                else:
                    sp_footnote = {}
                self.sp_sv[sp_code] = CDMGroup(sp_desc, sp_desc_tc, sp_alt_desc, sp_alt_desc_tc,
                                               type=sp_type, unit=unit, unit_desc=unit_desc, unit_desc_tc=unit_desc_tc,
                                               dec=dec, multi=multi, sep=sep, footnote=sp_footnote)
                # SV
                sv_df = sv_df.set_index('Common Data Model Code')
                for sv_code, sv_series in sv_df.iterrows():
                    sv_series.dropna(inplace=True)
                    sv_fas = sv_series['FAS field name']
                    sv_mdt = sv_series['FAS SP field name']
                    sv_desc = sv_series['FAS description']
                    sv_desc_tc = sv_series.get(
                        'FAS description Chinese', translator.translate(sv_desc))
                    verify = sv_desc.lower()
                    if 'Alternate' in sv_series:
                        sv_alt_desc = sv_series['Alternate']
                        sv_alt_desc_tc = sv_series.get('Alternate Chi', translator.translate(sv_alt_desc))
                        verify = sv_alt_desc.lower()
                    else:
                        sv_alt_desc = ''
                        sv_alt_desc_tc = ''

                    sv_footnote = fas_dict['SV'][sv_fas].get(verify, {})
                    self.sp_sv[sp_code][sv_code] = CDM(sv_desc, sv_desc_tc, sv_alt_desc, sv_alt_desc_tc,
                                                       fas=sv_fas, footnote=sv_footnote, mdt=sv_mdt)

    def get_sp_sv_id(self, desc, fas, sp_fas):
        return [
            [sv_result['id'], sv_result['child_id']]
            for sv_result in self.sp_sv.get_id_by_desc(desc, fas=fas, mdt=sp_fas)
        ]

    def init_mdt(self, theme, fas):
        for mdt_dict in fas.data:
            cvs_cc_id = {}
            all_cc_results = []
            multiple = []
            # split possibly multiple results
            for cc_fas, value in mdt_dict['CV'].items():
                cc_results = self.cv_cc.get_id_by_desc(value, 'parent_cc_code', 'ccg', fas=cc_fas)
                if len(cc_results) > 1:
                    multiple.extend(cc_results)
                else:
                    all_cc_results.extend(cc_results)

            cc_filter_df = pd.DataFrame(all_cc_results)
            # codes for non-multiple item(likely to be parent)
            if multiple:
                multiple_df = pd.DataFrame(multiple)
                # so that always child of child will be processed later and the code of parent will present
                multiple_df = multiple_df.sort_values(by='ccg')
                # insert the result if it's parent_cc_code is used in this record
                for _, row in multiple_df.iterrows():
                    if row['parent_cc_code'] in cc_filter_df['code'].values:
                        cc_filter_df = cc_filter_df.append(row)
            # group by id and compare their ccg value(max is child)
            cc_filter_df = cc_filter_df[cc_filter_df.groupby(['id'])['ccg'].transform(max) == cc_filter_df['ccg']]

            # convert the data into respective cv?_cc_id col and cc_id
            for _, cc_result in cc_filter_df.iterrows():
                cv_pos = int(re.search(r'\d+', theme[cc_result['id']]).group(0))
                cv_cc_id = f'[cv{cv_pos}_cc_id]'
                cvs_cc_id[cv_cc_id] = cc_result['child_id']
            #
            if cvs_cc_id:
                for sp_fas, value_dict in mdt_dict['MDT'].items():
                    obs_value = value_dict['obs_value']
                    sd_value = value_dict['sd_value']

                    sp_sv_id = []
                    if mdt_dict['SV']:
                        for sv_fas, sv_desc in mdt_dict['SV'].items():
                            sp_sv_id.extend(self.get_sp_sv_id(sv_desc, sv_fas, sp_fas))
                    else:
                        try:
                            for sv_desc in fas['SV']['undefined']:
                                sp_sv_id.extend(self.get_sp_sv_id(sv_desc, 'undefined', sp_fas))
                        except KeyError:
                            print('Something went wrong with SV, please check the FAS field of SV!')
                            # sys.exit(1)
                    if sp_sv_id:
                        for sp_id, sv_id in sp_sv_id:
                            insert_dict = {
                                '[theme_id]': theme.id,
                                '[sv_id]': sv_id,
                                '[sp_id]': sp_id,
                                '[obs_value]': obs_value,
                                '[sd_value]': sd_value
                            }
                            insert_dict.update(cvs_cc_id)
                            self.mdt.append(insert_dict)


class Theme(Dict, Connection):
    try:
        theme_dict = pd.read_csv('config\\theme.csv', dtype=str, index_col='THEME').to_dict()
    except FileNotFoundError:
        print('a file is not found in config!')
        sys.exit(1)

    #
    def __init__(self, theme_code):
        Dict.__init__(self)
        Connection.__init__(self)
        self.code = theme_code
        self.desc = type(self).theme_dict['THEME_DESC_ENG'][theme_code]
        self.desc_tc = type(self).theme_dict['THEME_DESC_CHI'][theme_code]
        self.id = 0

    def load_dict(self):
        try:
            # get cv1_id, cv2_id...cv20_id in THEME
            theme_df = self.select_sql(
                selector=', '.join([f'[cv{x}_id]' for x in range(1, 21)]),
                addr=f"{db_address['insert']}.[THEME]",
                where={
                    '[theme_id]': self.id
                },
                get_df=True,
                df_index=['cv_id']
            )
            # Transpose the df
            theme_df = theme_df.T
            # dropna
            theme_df = theme_df[pd.notnull(theme_df['cv_id'])]
            theme_df.index.name = 'cvs_id'
            theme_df.reset_index(level=0, inplace=True)
            theme_df.set_index('cv_id', inplace=True)
            theme_dict = theme_df['cvs_id'].to_dict()
            # convert to dictionary
            self.update(theme_dict)
        except AttributeError:
            pass

    def insert_cv_id(self, cv_id):
        # cv?_id col, ? is the current length + 1 so that it is the next one
        self.update_sql(
            addr=f"{db_address['insert']}.[THEME]",
            col=f'[cv{len(self) + 1}_id]',
            col_value=cv_id,
            condition_col='[theme_id]',
            condition_col_value=self.id
        )
        self.load_dict()


class Converter(Connection):
    out_df_dict = {}
    theme_df_dict = {}

    def __init__(self, theme_code, tb_code):
        Connection.__init__(self)
        self.theme_code = theme_code
        self.tb_code = tb_code
        self.df_dict = {}

    def save_df_dict(self):
        if self.theme_code not in type(self).out_df_dict:
            type(self).out_df_dict[self.theme_code] = {}
        type(self).out_df_dict[self.theme_code][self.tb_code] = self.df_dict

    @classmethod
    def merge_df(cls):
        for theme_code, table_df_dict in cls.out_df_dict.items():
            merged_df_dict = {}
            for df_dict in table_df_dict.values():
                for sheet_name, sheet in df_dict.items():
                    # if sheet.index.name:
                    #     sheet = sheet.reset_index()
                    if sheet_name not in merged_df_dict:
                        merged_df_dict[sheet_name] = pd.DataFrame()
                    if sheet_name == 'THEME' or sheet_name == 'SD':
                        merged_df_dict[sheet_name] = sheet
                    else:
                        merged_df_dict[sheet_name] = pd.concat(
                            [merged_df_dict[sheet_name], sheet], sort=False
                        )
            cls.theme_df_dict[theme_code] = merged_df_dict

    def process_part(self, table_name, get_field, insert_dict, df_col=None, concat=False,
                     additional_dict: dict = None, where_dict: dict = None):
        """
        Insert data from insert_dict into database
        Create a DataFrame from the inserted data
        :param table_name: table name of the Database. e.g. THEME, TB_INFO, CV, CC, SV...
        :param get_field: get field needs to be returned from this function. e.g. cv_id, sp_id, ccg_id...
        :param insert_dict: insert_dictionary
        :param df_col: additional df_col. e.g. cv1_id, cv2_id, cv3_id...
        :param concat: concat the newly created DataFrame and the old DataFrame in out_df
        :param additional_dict: additional dictionary
        :param where_dict: checking_dictionary
        :return: return the value of get_field
        """
        # if df_col has arg, then df_col = column in insert_dict + df_col
        if df_col:
            df_col = [col for col in insert_dict.keys() if col not in df_col] + df_col
        # if there an additional dictionary, update insert_dict
        if additional_dict:
            insert_dict.update(additional_dict)
        # if out_df[table_name] is not a DataFrame, create an empty DataFrame with df_col in out_df[table_name]
        if not isinstance(self.df_dict.get(table_name, 0), pd.DataFrame):
            self.df_dict[table_name] = pd.DataFrame(columns=df_col)
            if get_field != '1':
                self.df_dict[table_name].index.name = get_field
        addr = f"{db_address['insert']}.[{table_name}]"
        # get value from DB using select_sql method
        value = self.select_sql(
            selector=get_field,
            addr=addr,
            where=where_dict if where_dict else insert_dict
        )
        # If it returns nothing/0/None
        if not value:
            # insert using insert_dict into DB
            self.insert_sql(
                addr=addr,
                insert=insert_dict
            )
            # if get_field is 1, it check if it exist and do not need the value
            if get_field != '1':
                # get back the value using get_latest_id method
                value = self.get_latest_id(
                    addr=addr,
                    col=get_field
                )
            else:
                # for creating a DataFrame from insert_dict
                value = len(self.df_dict[table_name])
        # create a DataFrame from the inserted value and index is the value
        df = pd.DataFrame(insert_dict, index=[value], columns=df_col)
        if get_field != '1':
            df.index.name = get_field
        # if concat is enabled(in a loop of cv/sp), concat the current df and the existing df in out_df[table_name]
        if concat:
            self.df_dict[table_name] = pd.concat([self.df_dict[table_name], df],
                                                 sort=False,
                                                 ignore_index=True if get_field == '1' else False)
        else:
            # otherwise replace/set it
            self.df_dict[table_name] = df

        if get_field != '1':
            return value

    @staticmethod
    def write_excel(theme_code, df_dict, tb_code=''):
        filename = f"output\\{'_'.join([theme_code, tb_code]) if tb_code else theme_code}.xlsx"
        with pd.ExcelWriter(filename) as writer:
            for sheet_name, df in df_dict.items():
                index = False
                if df.index.name:
                    index = True
                    # drop possibly duplicated indices in the current DataFrame
                    df = df[~df.index.duplicated(keep='first')]
                    df.index.name = df.index.name[1:-1] if df.index.name[0] == '[' else df.index.name
                # drop possibly duplicated rows in the current DataFrame
                df = df.drop_duplicates(keep='first')
                # strip the bracket
                df.columns = [column[1:-1] if column[0] == '[' else column for column in df.columns]
                # if that DataFrame has index name i.e. has unique id column
                df.to_excel(writer, sheet_name=sheet_name, index=index)
        print(f'Written to {filename}.')

    @classmethod
    def convert_table(cls):
        for theme_code, table_df_dict in cls.out_df_dict.items():
            for tb_code, df_dict in table_df_dict.items():
                cls.write_excel(theme_code, df_dict, tb_code)

    @classmethod
    def convert_theme(cls):
        if cls.theme_df_dict:
            for theme_code, merged_df_dict in cls.theme_df_dict.items():
                cls.write_excel(theme_code, merged_df_dict)
        else:
            print('Please run class method merge_df first!')


class Translator(Connection):
    try:
        unit_dict = pd.read_csv(
            'config\\unit.csv', dtype=str, index_col='Unit_desc_eng'
        )['Unit_desc_chi'].str.lower().to_dict()
    except FileNotFoundError:
        print('unit.csv is not found in config!')
        sys.exit(1)

    #
    def __init__(self, tb_code):
        Connection.__init__(self)
        self.tb_code = tb_code
        self.table_field_dict = {}
        self.all_field_dict = {}

    def load_data(self):
        table_field_df = self.select_sql(
            selector="[table_id] tb_code, LOWER(REPLACE(desc_eng, '<br>', '')) desc_eng, "
                     "REPLACE (desc_chi, '<br>', '') desc_chi",
            addr=f"{db_address['reference']}.[TB_FIELDLOOKUP]",
            where={'[table_id]': self.tb_code},
            get_df=True
        )
        table_field_df = table_field_df[['desc_eng', 'desc_chi']].set_index('desc_eng')
        self.table_field_dict = table_field_df.to_dict()['desc_chi']
        #
        all_field_df = self.select_sql(
            replace_sql=(
                "SELECT desc_eng, desc_chi, COUNT(*) occurrence FROM ("
                "SELECT LOWER(REPLACE(desc_eng, '<br>', '')) desc_eng, REPLACE(desc_chi, '<br>', '') desc_chi FROM "
                f"{db_address['reference']}.[TB_FIELDLOOKUP]) AS T GROUP BY desc_eng, desc_chi"
            ),
            get_df=True
        )
        all_field_df = all_field_df[
            all_field_df['occurrence'] == all_field_df.groupby('desc_eng', sort=False)['occurrence'].transform('max')
            ]
        all_field_df = all_field_df[['desc_eng', 'desc_chi']].set_index('desc_eng')
        self.all_field_dict = all_field_df.to_dict()['desc_chi']

    def translate(self, desc_eng, is_unit=False):
        check = self.all_field_dict if not is_unit else type(self).unit_dict
        if not is_unit:
            desc_eng = desc_eng.lower()
        # if it exists in the list filtered by table code
        variants = (desc_eng, desc_eng.replace(' (', '('), desc_eng.replace('(', ' ('))
        for variant in variants:
            if variant in check:
                return check[variant]
            elif variant in self.table_field_dict:
                return '(NOT IN FIELDLOOKUP)' + self.table_field_dict[variant]
            # make sure last one is checked
            elif variant == variants[-1]:
                return 'NOT FOUND'


class Fas(Dict, Connection):
    def __init__(self, tb_code, cdm_df_dict):
        Dict.__init__(self)
        Connection.__init__(self)
        self.tb_code = tb_code
        self.cdm_df_dict = cdm_df_dict
        self.sd = SD()
        self.footnote = Footnote(tb_code)
        self.columns = []
        self.data = []
        self.df = None

    def load_columns(self):
        self.columns = [
            [fas_name, f'{fas_name}_footnote']
            for fas_name in self.all_fas_names() if fas_name != 'undefined'
        ]

    def load_fas_df(self):
        try:
            fas_df = self.select_sql(
                selector=','.join(
                    chain.from_iterable(
                        (f"REPLACE([{col_fas_name}], '<br>', '') [{col_fas_name}]", f'[{col_fas_footnote}]')
                        for col_fas_name, col_fas_footnote in self.columns
                    )
                ),
                addr=f"{db_address['reference']}.[TABLE{self.tb_code}]",
                get_df=True
            )
            fas_df.replace('', np.nan, inplace=True)
            # in case of typos :)
            fas_df.replace('N.A', 'N.A.', inplace=True)
            self.df = fas_df
        except pymssql.ProgrammingError:
            print('Database does not have an fas listed in CSV! Error!')
            sys.exit(1)

    def parse_csv_dict(self):
        for field, field_df in self.cdm_df_dict.items():
            self[field] = {}
            for fas_name, fas_df in field_df.groupby('FAS field name', sort=False):
                self[field][fas_name] = {}
                if field != 'MDT':
                    for _, row in fas_df.iterrows():
                        row = row.apply(lambda x: str(x).lower())
                        if field == 'CV':
                            desc = 'CC Description'
                            alt_desc = 'CC Alternate'
                        else:
                            desc = 'FAS description'
                            alt_desc = 'Alternate'
                        self[field][fas_name].update({row[desc]: {}})
                        if alt_desc in row.dropna():
                            self[field][fas_name].update({row[alt_desc]: {}})

    def update_footnote(self, field, fas_name, desc, footnote_no, footnote_desc, footnote_desc_tc):
        try:
            self[field][fas_name][desc.lower()][f'[fn{footnote_no}_en]'] = footnote_desc
            self[field][fas_name][desc.lower()][f'[fn{footnote_no}_tc]'] = footnote_desc_tc
        except KeyError:
            print(desc + ' is not in ' + fas_name)

    def all_fas_names(self):
        return set(chain.from_iterable(self.values()))

    def all_fas_desc(self):
        return [
            desc
            for fas_names in self.values()
            for item in fas_names.values()
            for desc in item
        ]

    def update_footnote_and_parse_fas_df(self):
        for i, row in self.df.iterrows():
            row.dropna(inplace=True)
            for col_fas_name, col_fas_footnote in self.columns:
                if col_fas_name in row and col_fas_footnote in row:
                    desc = row[col_fas_name]
                    notes = row[col_fas_footnote]
                    for field, fas_names in self:
                        if col_fas_name in fas_names:
                            if desc.lower() in fas_names[col_fas_name]:
                                notes = re.sub('[()]', '', notes)
                                for note_no, note in enumerate(notes, start=1):
                                    if field == 'MDT':
                                        if note in self.footnote:
                                            self[field][col_fas_name][desc.lower()] = self.sd[note]
                                        else:
                                            self.sd.update_sd(note, self.footnote[note]['NOTE_ENG'],
                                                              self.footnote[note]['NOTE_CHI'])
                                    else:
                                        if note in self.footnote:
                                            self.update_footnote(field, col_fas_name, desc, note_no,
                                                                 self.footnote[note]['NOTE_ENG'],
                                                                 self.footnote[note]['NOTE_CHI'])
            #
            for sp_fas in self['MDT'].keys():
                if sp_fas in row:
                    row[sp_fas] = row[sp_fas].replace(' ', '').replace(',', '')
                    value = row[sp_fas]
                    try:
                        float(value)
                    except ValueError:
                        if value not in self.sd.keys():
                            self.sd.update_sd(value,
                                              self.footnote[value]['NOTE_ENG'], self.footnote[value]['NOTE_CHI'])
            #
            row = row[~row.index.str.endswith('footnote')]
            mdt_dict = {'CV': {}, 'SV': {}, 'MDT': {}}
            insert = True
            for fas_name, value in row.items():
                if fas_name in self.all_fas_names():
                    if value.lower() in self.all_fas_desc() or fas_name in self['MDT']:
                        for field, fas_names in self:
                            if fas_name in fas_names:
                                if field == 'MDT':
                                    mdt_dict[field][fas_name] = {}
                                    try:
                                        mdt_dict[field][fas_name]['obs_value'] = float(value)
                                        mdt_dict[field][fas_name]['sd_value'] = self['MDT'][fas_name].get(value, 0)
                                    except ValueError:
                                        mdt_dict[field][fas_name]['obs_value'] = 0
                                        mdt_dict[field][fas_name]['sd_value'] = self.sd[value]
                                elif value.lower() in fas_names[fas_name]:
                                    mdt_dict[field][fas_name] = value
                    else:
                        print(f'fas desc - {value} from TABLE{self.tb_code} is not used in CSV')
                        insert = False
                else:
                    print(f'fas - {fas_name} from TABLE{self.tb_code} is not used in CSV')
                    insert = False
            if not mdt_dict['CV']:
                print('CV is empty for a row, skipped')
                insert = False
            if insert:
                self.data.append(mdt_dict)


class SD(Dict, Connection):
    def __init__(self):
        Dict.__init__(self)
        Connection.__init__(self)
        self.df = None

    def load_sd(self):
        df = self.select_sql(
            selector='*',
            addr=f"{db_address['insert']}.[SD]",
            get_df=True
        )
        self.df = df[['sd_value', 'sd_symbol', 'sd_desc_eng', 'sd_desc_chi', 'sd_suppressed']]
        self.update(df[['sd_value', 'sd_symbol']].set_index('sd_symbol')['sd_value'].to_dict())

    def update_sd(self, sd_footnote, sd_footnote_desc, sd_footnote_desc_tc, suppressed=False):
        sd_value = max(x for x in self.values() if x < 90) + 1
        insert = {
            '[sd_value]': sd_value,
            '[sd_symbol]': sd_footnote,
            '[sd_desc_eng]': sd_footnote_desc,
            '[sd_desc_chi]': sd_footnote_desc_tc
        }
        if suppressed:
            insert.update({'[sd_suppressed]': 1})
        self.insert_sql(
            addr=f"{db_address['insert']}.[SD]",
            insert=insert
        )
        self.load_sd()


class Footnote(Dict, Connection):
    try:
        unused_note_dict = {
            tb_code: note['NOTE'].to_list()
            for tb_code, note in pd.read_csv('config\\table_info.csv', dtype=str).groupby('Table')
        }
    except FileNotFoundError:
        print('table_info.csv is not found in config!')
        sys.exit(1)

    def __init__(self, tb_code):
        Dict.__init__(self)
        Connection.__init__(self)
        self.tb_code = tb_code
        self.info_footnotes_df = None
        self.table_info_note_list = type(self).unused_note_dict.get(self.tb_code, [])

    def load_footnote(self):
        def filter_footnotes(note_type):
            """
            process a list and check if NOTE_TYPE of this item and the next two items are 1, 2, 3
            :param note_type: a NOTE_TYPE item object
            :return: return an index list of rows that should be removed
            """
            remove_list = []
            count_list = []
            while True:
                try:
                    count_list.append(next(note_type))
                    if len(count_list) == 3:
                        if all(count_list[__i][1] == __i + 1 for __i in range(3)):
                            #
                            remove_list.extend(count_list[1:])
                            count_list = []
                        else:
                            # remove the first item in order to load a new item
                            del count_list[0]
                except StopIteration:
                    break
            return [row_index for row_index, note_type in remove_list]
        #
        footnotes_df = self.select_sql(
            selector='[NOTE_NO], [NOTE], [NOTE_ENG], [NOTE_CHI], [NOTE_TYPE]',
            addr=f"{db_address['reference']}.[TB_FOOTNOTE]",
            where={
                '[TABLE_ID]': self.tb_code
            },
            get_df=True,
            addition=' AND [NOTE_TYPE] < 4 ORDER BY [NOTE_NO]'
        )
        footnotes_df = footnotes_df.astype({'NOTE_NO': 'int32', 'NOTE_TYPE': 'int32'})
        footnotes_filter = filter_footnotes(
            footnotes_df[footnotes_df['NOTE_TYPE'] != 0]['NOTE_TYPE'].items())
        footnotes_df.drop(footnotes_filter, inplace=True)
        footnotes_df.drop_duplicates(['NOTE_CHI', 'NOTE_ENG'], inplace=True)
        footnotes_df.reset_index(drop=True, inplace=True)
        #
        note_df = footnotes_df[footnotes_df['NOTE'] != ''].set_index('NOTE')
        # Since the 'NOTE' must be unique, select the max of 'NOTE_TYPE' in 'NOTE' group
        note_df = note_df[
            note_df['NOTE_TYPE'] == note_df.groupby('NOTE', sort=False)['NOTE_TYPE'].transform('max')
        ]
        note_df = note_df[['NOTE_ENG', 'NOTE_CHI']]
        note_df.drop_duplicates(inplace=True)
        note_dict = note_df.to_dict(orient='index')
        note_dict = {
            # strip the circle brackets in footnote symbols
            re.sub('[()]', '', key): value
            for key, value in note_dict.items()
        }
        self.info_footnotes_df = pd.concat(
            [
                footnotes_df[footnotes_df['NOTE'] == ''],
                footnotes_df[
                    footnotes_df['NOTE'].isin(self.table_info_note_list)
                ]
            ]
        )
        self.update(note_dict)

    def parse(self, header, src=False, tc=False):
        col = 'NOTE_CHI' if tc else 'NOTE_ENG'
        fn_df = self.info_footnotes_df[
            self.info_footnotes_df['NOTE_NO'] == 99] if src else self.info_footnotes_df[
            self.info_footnotes_df['NOTE_NO'] != 99]
        txt = ''
        for __i, __row in fn_df.iterrows():
            # in case there's a bracket
            s = __row[col].replace('\n', '').replace('<br>', '\n')
            if __row['NOTE']:
                s = __row['NOTE'] + ' ' + s
            txt += s
            if __i is not int(fn_df.index[-1]):
                txt += '\n'
        return header + txt if txt else ''
