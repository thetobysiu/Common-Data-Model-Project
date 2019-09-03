#!/usr/bin/env python3
# coding=UTF-8
import argparse
import os
from classes import *


def process_table(path):
    table = Table()
    table.load_csv(path)
    table.parse_config_df()
    table.parse_cdm_df()
    theme = Theme(table.get_theme_code())
    print(table.code, theme.code, theme.desc, theme.desc_tc, table.title, table.title_tc)

    converter = Converter(theme.code, table.code)

    translator = Translator(table.code)
    translator.load_data()

    fas = Fas(table.code, table.dict)
    fas.parse_csv_dict()
    fas.sd.load_sd()
    fas.footnote.load_footnote()
    table.parse_footnote(fas.footnote)

    fas.load_columns()
    fas.load_fas_df()
    fas.update_footnote_and_parse_fas_df()

    # TB_INFO - get tb_id
    table.id = converter.process_part(
        table_name='TB_INFO',
        get_field='[tb_id]',
        where_dict={
            '[tb_code]': table.code
        },
        insert_dict={
            '[tb_code]': table.code,
            '[tb_title_en]': table.title,
            '[tb_title_tc]': table.title_tc,
            '[tb_fn_en]': table.fn,
            '[tb_fn_tc]': table.fn_tc,
            '[tb_src_en]': table.src,
            '[tb_src_tc]': table.src_tc
        }
    )

    # THEME - get theme_id
    theme.id = converter.process_part(
        table_name='THEME',
        get_field='[theme_id]',
        where_dict={
            '[theme]': theme.code
        },
        insert_dict={
            '[theme]': theme.code,
            '[theme_desc_en]': theme.desc,
            '[theme_desc_tc]': theme.desc_tc
        },
        df_col=[f'[cv{i}_id]' for i in range(1, 21)]
    )
    theme.load_dict()

    #
    converter.df_dict['SD'] = fas.sd.df

    #
    print('[--CV & CC--]')
    table.init_cv_cc(translator, fas.dict)
    print(table.cv_cc)
    for cv_code, cv in table.cv_cc:
        # CV - get cv_id
        cv_id = converter.process_part(
            table_name='CV',
            get_field='[cv_id]',
            where_dict={
                '[class_var]': cv_code,
                '[theme_id]': theme.id
            },
            insert_dict={
                '[theme_id]': theme.id,
                '[class_var]': cv_code,
                '[def_class_desc_en]': cv.desc,
                '[def_class_desc_tc]': cv.desc_tc
            },
            concat=True
        )
        table.cv_cc.update_cdm(
            cv_code, id=cv_id
        )
        # CV_TB
        converter.process_part(
            table_name='CV_TB',
            get_field='1',
            where_dict={
                '[tb_id]': table.id,
                '[cv_id]': cv_id
            },
            insert_dict={
                '[cv_id]': cv_id,
                '[tb_id]': table.id,
                '[class_desc_en]': cv.get_tb_desc(),
                '[class_desc_tc]': cv.get_tb_desc(tc=True)
            },
            concat=True,
            df_col=list(chain.from_iterable([[f'[fn{i}_en]', f'[fn{i}_tc]'] for i in range(1, 6)]))
        )
        for cc_code, cc in cv:
            # CCG - get ccg_id
            ccg_id = converter.process_part(
                table_name='CCG',
                get_field='[ccg_id]',
                insert_dict={
                    '[cv_id]': cv_id,
                    '[class_code_group]': cc.ccg
                },
                concat=True
            )
            table.cv_cc.update_cdm_child(
                cv_code, cc_code, ccg_id=ccg_id
            )
            # CC - get cc_id
            cc_id = converter.process_part(
                table_name='CC',
                get_field='[cc_id]',
                where_dict={
                    '[cv_id]': cv_id,
                    '[class_code]': cc_code
                },
                insert_dict={
                    '[cv_id]': cv_id,
                    '[class_code]': cc_code,
                    '[def_class_code_desc_en]': cc.desc,
                    '[def_class_code_desc_tc]': cc.desc_tc
                },
                concat=True
            )
            table.cv_cc.update_cdm_child(
                cv_code, cc_code, id=cc_id
            )
            # CCG_CC
            converter.process_part(
                table_name='CCG_CC',
                get_field='1',
                where_dict={
                    '[ccg_id]': ccg_id,
                    '[cc_id]': cc_id
                },
                insert_dict={
                    '[ccg_id]': ccg_id,
                    '[cc_id]': cc_id,
                    '[cv_id]': cv_id,
                    '[class_code_seq]': cc.seq
                },
                concat=True
            )
            # CC_TB
            converter.process_part(
                table_name='CC_TB',
                get_field='1',
                where_dict={
                    '[cc_id]': cc_id,
                    '[tb_id]': table.id
                },
                insert_dict={
                    '[cc_id]': cc_id,
                    '[tb_id]': table.id,
                    '[class_code_desc_en]': cc.get_tb_desc(),
                    '[class_code_desc_tc]': cc.get_tb_desc(tc=True),
                    '[ccg_id]': ccg_id
                },
                concat=True,
                additional_dict=cc.footnote,
                df_col=list(chain.from_iterable([[f'[fn{i}_en]', f'[fn{i}_tc]'] for i in range(1, 6)]))
            )
        # PAC
        for cc_code, cc in cv:
            if cc.parent_cc_code:
                parent = cv[cc.parent_cc_code]
                converter.process_part(
                    table_name='PAC',
                    get_field='1',
                    insert_dict={
                        '[parent_ccg_id]': parent.ccg_id,
                        '[parent_cc_id]': parent.id,
                        '[child_ccg_id]': cc.ccg_id,
                        '[child_cc_id]': cc.id
                    },
                    concat=True
                )
        if 'PAC' not in converter.df_dict:
            converter.df_dict['PAC'] = pd.DataFrame(
                columns=['[parent_ccg_id]', '[parent_cc_id]', '[child_ccg_id]', '[child_cc_id]']
            )

    #
    print('[--THEME - cv(s)_ id--]')
    # update the processed newly assigned cv_id to THEME
    for cv_id in table.cv_cc.all_ids():
        if cv_id not in theme:
            theme.insert_cv_id(cv_id)
    for cv_id, col, in theme:
        # create the DataFrame in out_df, where the index is the theme id and the column is the col respective cv?_id
        converter.df_dict['THEME'].loc[theme.id, f'[{col}]'] = cv_id

    #
    print('[--SP & SV related-]')
    table.init_sp_sv(translator, fas.dict)
    print(table.sp_sv)
    # print(sp_sv)
    for sp_code, sp in table.sp_sv:
        # SP - get sp_id
        sp_id = converter.process_part(
            table_name='SP',
            get_field='[sp_id]',
            where_dict={
                '[stat_pres]': sp_code,
                '[def_stat_pres_desc_en]': sp.desc,
                '[def_stat_pres_desc_tc]': sp.desc_tc,
                '[theme_id]': theme.id
            },
            insert_dict={
                '[stat_pres]': sp_code,
                '[theme_id]': theme.id,
                '[def_stat_pres_desc_en]': sp.desc,
                '[def_stat_pres_desc_tc]': sp.desc_tc,
                '[def_stat_type]': sp.type,
                '[def_unit]': sp.unit,
                '[def_unit_desc_en]': sp.unit_desc,
                '[def_unit_desc_tc]': sp.unit_desc_tc,
                '[def_decimals]': sp.dec,
                '[def_unit_mult]': sp.multi,
                '[def_separator_format]': sp.sep
            },
            concat=True
        )
        table.sp_sv.update_cdm(sp_code, id=sp_id)
        # SP_TB
        converter.process_part(
            table_name='SP_TB',
            get_field='1',
            where_dict={
                '[sp_id]': sp_id,
                '[tb_id]': table.id
            },
            insert_dict={
                '[sp_id]': sp_id,
                '[tb_id]': table.id,
                '[stat_pres_desc_en]': sp.get_tb_desc(),
                '[stat_pres_desc_tc]': sp.get_tb_desc(tc=True),
                '[stat_type]': sp.type,
                '[unit]': sp.unit,
                '[unit_desc_en]': sp.unit_desc,
                '[unit_desc_tc]': sp.unit_desc_tc,
                '[decimals]': sp.dec,
                '[unit_mult]': sp.multi,
                '[separator_format]': sp.sep
            },
            concat=True,
            additional_dict=sp.footnote,
            df_col=list(chain.from_iterable([[f'[fn{i}_en]', f'[fn{i}_tc]'] for i in range(1, 6)]))
        )
        for sv_code, sv in sp:
            # SV - get sv_id
            sv_id = converter.process_part(
                table_name='SV',
                get_field='[sv_id]',
                where_dict={
                    '[theme_id]': theme.id,
                    '[stat_var]': sv_code
                },
                insert_dict={
                    '[theme_id]': theme.id,
                    '[stat_var]': sv_code,
                    '[def_stat_desc_en]': sv.desc,
                    '[def_stat_desc_tc]': sv.desc_tc
                },
                concat=True
            )
            table.sp_sv.update_cdm_child(sp_code, sv_code, id=sv_id)
            # SV_TB
            converter.process_part(
                table_name='SV_TB',
                get_field='1',
                where_dict={
                    '[sv_id]': sv_id,
                    '[tb_id]': table.id
                },
                insert_dict={
                    '[sv_id]': sv_id,
                    '[tb_id]': table.id,
                    '[stat_desc_en]': sv.get_tb_desc(),
                    '[stat_desc_tc]': sv.get_tb_desc(tc=True)
                },
                concat=True,
                additional_dict=sv.footnote,
                df_col=list(chain.from_iterable([[f'[fn{i}_en]', f'[fn{i}_tc]'] for i in range(1, 6)]))
            )

    #
    print('[--MDT--]')
    table.init_mdt(theme, fas)
    for i, insert_dict in enumerate(table.mdt):
        # MDT - get mtd_id
        mdt_id = converter.process_part(
            table_name='MDT',
            get_field='[mdt_id]',
            insert_dict=insert_dict,
            df_col=[f'[cv{i}_cc_id]' for i in range(1, 21)],
            concat=True
        )
        table.mdt[i]['[mdt_id]'] = mdt_id

    #
    print('[--TB_COMP--]')
    # SV and SP used
    for sp_id, sv_id in table.sp_sv.all_ids(include_child=True):
        converter.process_part(
            table_name='TB_COMP',
            get_field='1',
            insert_dict={
                '[tb_id]': table.id,
                '[sv_id]': sv_id,
                '[sp_id]': sp_id
            },
            concat=True
        )
    # CCG used
    for ccg_id in table.cv_cc.all_ccg():
        converter.process_part(
            table_name='TB_COMP',
            get_field='1',
            insert_dict={
                '[tb_id]': table.id,
                '[ccg_id]': ccg_id
            },
            concat=True
        )
    converter.save_df_dict()


def main():
    # add an arg parser to process args
    parser = argparse.ArgumentParser(usage='--file [CSV/XLSX path] / --folder [folder path]')
    # add an exclusive group so that either file other folder arg will be accepted
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--file', help='File mode to process a single file')
    group.add_argument('--folder', help='Process all files in a folder')
    args = parser.parse_args()
    #
    # if its file mode
    if args.file:
        print('----File mode----')
        file_name = args.file
        process_table(file_name)
        Converter.convert_table()
    # if its folder mode
    elif args.folder:
        print('----Folder mode----')
        folder_name = args.folder
        file_list = [
            f'{folder_name}\\{file_name}'
            for file_name in os.listdir(folder_name)
            if file_name.lower().endswith(".csv") or file_name.lower().endswith(".xlsx")
        ]
        for file_name in file_list:
            process_table(file_name)
        Converter.merge_df()
        Converter.convert_table()
        Converter.convert_theme()


if __name__ == '__main__':
    main()
