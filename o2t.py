#!/usr/bin/env python
# -*- coding:utf-8 -*-

import re
import cx_Oracle
import sys
import threading
import sqlite3
import os, argparse
import ast
import json
import itertools
import configparser
import datetime
import pymysql
import psycopg2

class Trans_Ora_DDL:
    """
        Separate treatment column, primary, comment, partition.etc 
        And then splice it together
    """
    def __init__(self, **param):
        self.connect=''
        self.cursor=''
        self.dbtype = param['dbtype']
        self.owner = param['owner']
        self.directory = param['directory']
        self.content = param['content']
        
        # Get information from sqlite
        sql_get_rule = """
                        SELECT
                            rulematch, rulereplace, ruleparameters
                        FROM
                            o2t_rule
                        WHERE
                            dbtype='{0}'
                        AND
                            ruletype='datatype'
                       """.format(self.dbtype) 
        
        sql_get_default = """
                            SELECT
                                rulematch, rulereplace
                            FROM
                                o2t_rule
                            WHERE
                                dbtype = '{0}'
                            AND
                                ruletype <> 'datatype'
                          """.format(self.dbtype) 
        
        try:
            sqlitedb = sqlite3.connect("o2t.db")
            sqlitecursor = sqlitedb.cursor()
            sqlitecursor.execute(sql_get_rule)
            self.rulelist = sqlitecursor.fetchall()
            sqlitecursor.execute(sql_get_default)
            self.keywordlist = sqlitecursor.fetchall()
            sqlitedb.close()
        except Exception as err:
            print(err)
            sys.exit()
        
        # Get keyword
        target_db = pymysql.connect(**target_conn) if self.dbtype == 'tdsql' else psycopg2.connect(**target_conn) 
        sql_get_keyword = """
                            SELECT  
                                lower(word)
                            FROM
                          """ + (' information_schema.keywords WHERE reserved = 1' if self.dbtype == 'tdsql' else " pg_get_keywords() where catdesc = 'reserved'")
        
        try:
            with target_db.cursor()as target_cur:
                target_cur.execute(sql_get_keyword)
                self.reservewords_list = [i[0] for i in target_cur.fetchall()]
        except Exception as err:
            print(err)
            sys.exit()
        finally:
            target_db.close()
     
    def get_table_list(self, options, tablefile):
        """
        Check whether all export tables exist.
        if dbtype = tdsql, Check whether all export tables have primary key.
        """

        transf_table_list = []  

        if options == 'include': 
            with open(tablefile, 'r') as f: 
                check_table_list = [table_name.strip() for table_name in f if not re.search(r'^#', table_name)] 
            # Check whether all tables in tablefile exist in the database
            sql_check_exist = """
                                SELECT 
                                    table_name 
                                FROM 
                                    all_tables
                                WHERE 
                                    owner = '{0}'
                                AND
                                    table_name in {1}
                              """.format(self.owner, \
                                         str(tuple(check_table_list)) if len(check_table_list) > 1 else str(tuple(check_table_list)).replace(',',''))
            
            self.cursor.execute(sql_check_exist)
            check_exist_list = [ i[0] for i in self.cursor.fetchall() ]

            for table_name in check_table_list:
                if table_name not in check_exist_list:
                    print('No such table: {0}'.format(table_name))
                    sys.exit()
        elif options == 'exclude':
            with open(tablefile, 'r') as f: 
                 check_table_list = [table_name.strip() for table_name in f if not re.search(r'^#', table_name)]
            sql_get_table = """
                                SELECT 
                                    table_name 
                                FROM 
                                    all_tables
                                WHERE
                                    owner = '{0}'
                                AND
                                    table_name not in{1}
                            """.format(self.owner,  \
                                       str(tuple(check_table_list)) if len(check_table_list) > 1 else str(tuple(check_table_list)).replace(',',''))
            
            self.cursor.execute(sql_get_table)
            check_table_list = [ i[0] for i in self.cursor.fetchall() ]
        
        # Check whether all tables have primary keys
        if self.content != 'index':
            if self.dbtype == 'tdsql':
                if options:
                    sql_check_primary = """
                                            SELECT 
                                                table_name
                                            FROM
                                                all_tables
                                            WHERE   
                                                owner = '{0}'
                                            AND 
                                                table_name in{1}
                                            AND 
                                                table_name not in (
                                            SELECT /*+ UNNEST */ 
                                                table_name 
                                            FROM 
                                                all_constraints
                                            WHERE 
                                                constraint_type = 'P' 
                                            AND
                                                owner = '{0}'
                                            AND 
                                                status <> 'DISABLED')
                                        """.format(self.owner, \
                                                   str(tuple(check_table_list)) if len(check_table_list) > 1 else str(tuple(check_table_list)).replace(',',''))
                else:
                    sql_check_primary = """
                                            SELECT 
                                                table_name 
                                            FROM 
                                                all_tables
                                            WHERE 
                                                owner = '{0}'
                                            AND 
                                                table_name not in (
                                            SELECT /*+ UNNEST */ 
                                                table_name 
                                            FROM 
                                                all_constraints
                                            WHERE 
                                                constraint_type = 'P' 
                                            AND
                                                owner = '{0}'
                                            AND 
                                                status <> 'DISABLED')
                                        """.format(self.owner)

                self.cursor.execute(sql_check_primary)
                no_primary_list = [i[0] for i in self.cursor.fetchall()]

                if len(no_primary_list) > 0:
                    print(str(no_primary_list) + ' does not have primary key')
                    sys.exit()
        
        # Get table list
        sql_get_table_list = """
                                SELECT  
                                    owner, table_name, partitioned
                                FROM
                                    all_tables 
                                WHERE
                                    owner = '{0}'
                             """.format(self.owner) + \
                                (('AND table_name in {0}'.format(str(tuple(check_table_list)) if len(check_table_list) > 1 else str(tuple(check_table_list)).replace(',','')) if options else ''))
        
        self.cursor.execute(sql_get_table_list)
        transf_table_list = [i for i in self.cursor.fetchall()]

        return transf_table_list
    
    def transf_column(self, tablename):
        """
        Transfer column type and column comment to others.
        """
        column_sql = ''
        comment_sql = ''
        # Construct column
        sql_get_column = """
                            SELECT
	                            a.column_name,
	                            data_type,
	                            CASE
		                            WHEN CHAR_COL_DECL_LENGTH IS NULL THEN data_length
		                            ELSE CHAR_LENGTH
	                            END AS datalength,
	                            data_precision,
                                CASE
                                    WHEN data_precision IS NULL AND DATA_SCALE IS NOT NULL THEN DATA_SCALE
                                    ELSE NULLIF(data_scale, 0)
	                            END AS data_scale,
	                            nullable,
	                            b.comments,
	                            virtual_column,
                                DATA_DEFAULT
                            FROM
	                            all_tab_cols a,
	                            all_col_comments b
                            WHERE
	                            a.table_name = b.table_name
	                            AND 
                                    a.owner = b.owner
	                            AND 
                                    a.column_name = b.column_name
	                            AND 
                                    a.owner = '{0}'
	                            AND
                                    a.table_name = '{1}'
                                ORDER BY
	                                column_id
                        """.format(self.owner, tablename)

        self.cursor.execute(sql_get_column)
        column_info_list = self.cursor.fetchall()
        for column_info in column_info_list:
            column_str = ''

            # Construct not null constraint
            if column_info[5] == 'N':
                column_str = column_str + ' not null'
            
            # Construct the column comment 
            if column_info[6] is not None: 
                if self.dbtype == 'tdsql':
                    column_str = column_str + " comment '" + column_info[6] + "'"
                elif self.dbtype == 'tbase':
                    comment_sql = comment_sql + 'comment on column ' + tablename.lower() + '.' + \
                                  column_info[0].lower() + " is '" + column_info[6] + "';\n"
            
            # Construct column regex
            if re.search(r'\(\d\)', column_info[1]):
                column_type = column_info[1]
            elif re.search(r'char|raw|urowid', column_info[1], re.IGNORECASE):
                column_type = column_info[1] + '(' + str(column_info[2]) + ')'
            elif column_info[4] is None:
                if column_info[3] is None:
                    column_type = column_info[1]
                else:
                    column_type = column_info[1] + '(' + str(column_info[3]) + ')'
            elif column_info[4] is not None:
                if column_info[3] is None:
                    column_type = column_info[1] + '(*,' + str(column_info[4]) + ')'
                else:
                    column_type = column_info[1] + '(' + str(column_info[3]) + ',' + str(column_info[4]) + ')'
            else:
                column_type = column_info[1] + '(' + str(column_info[3]) + ',' + str(column_info[4]) + ')'
            
            # Transfer column
            transf_column_type =  column_type.lower()
            for rule in self.rulelist:
                break_flag = False
                rulematch = "^" + rule[0].strip() + "$"
                regex_result = re.match(rulematch,  column_type, re.IGNORECASE)
                if regex_result:
                    data_list = regex_result.groups()
                    if len(data_list) == 0:
                        transf_column_type = rule[1]
                        break
                    else:
                        if rule[2] == '':
                            transf_column_type = rule[1].format(*data_list)
                            break
                        else:
                            ruleparameters = ast.literal_eval(rule[2])
                            for data_list_num, value_dict in ruleparameters.items():
                                for cond_str, cond_value in value_dict.items():
                                    if cond_str == 'max':
                                        if int(data_list[data_list_num]) <= cond_value:
                                            continue
                                        else:
                                            break_flag = True
                                            break
                                    elif cond_str == 'min':
                                        if int(data_list[data_list_num]) >= cond_value:
                                            continue
                                        else:
                                            break_flag = True
                                            break

                                if break_flag == True:
                                    break
                                else:
                                    transf_column_type = rule[1].format(*data_list)
            
            if column_info[0].lower() in self.reservewords_list or len(column_info[0].split()) > 1:
                if self.dbtype == 'tbase':
                    column_name = '"' + column_info[0].lower() + '"'
                elif self.dbtype == 'tdsql':
                    column_name = '`' + column_info[0].lower() + '`' 
            else:
                column_name = column_info[0].lower()
            
            if column_info[8] is not None:
                default_sql = column_info[8].strip()
                if re.search(r"^'.*'$", default_sql):
                    default_sql = ' default ' + default_sql
                else:
                    try:
                        float(default_sql)
                        default_sql = ' default ' + default_sql
                    except Exception:
                        if self.dbtype == 'tdsql':
                            if re.search(r'timestamp', default_sql, re.IGNORECASE) or re.search(r'date', default_sql, re.IGNORECASE):
                                if default_sql.lower() == 'sysdate':
                                    default_sql = ' default current_timestamp'
                                elif default_sql.lower() == 'systimestamp':
                                    default_sql = ' default current_timestamp'
                                else:
                                    default_sql = ' default ' + default_sql
                            else:
                                default_sql = '' 
                        elif self.dbtype == 'tbase':
                            get_space = re.search(r'(\s*,\s*)', default_sql)
                            if get_space:
                                for space in get_space.groups():
                                    default_sql = default_sql.replace(space, ',')

                            for keyword in self.keywordlist:
                                keywordmatch = "^" + keyword[0] + "$"
                                keyword_regex = re.match(keywordmatch, default_sql, re.IGNORECASE)
                                if keyword_regex:
                                    data_list = keyword_regex.groups()
                                    if len(data_list) == 0:
                                        default_sql = ' default ' + keyword[1]
                                        break
                                    else:
                                        default_sql = ' default ' + keyword[1].format(**data_list)
                                        break
                                else:
                                    continue

                if default_sql == column_info[8] :
                    default_sql = ' default ' + default_sql
                
                column_sql = column_sql + '    ' + column_name + ' ' + transf_column_type + default_sql + column_str + ',\n'
            else:
                column_sql = column_sql + '    ' + column_name + ' ' + transf_column_type + column_str + ',\n'
        
        return column_sql.rstrip(',\n'), comment_sql
        
    def transf_primary(self, tablename, partitioned):      
        """
        Construct primary key to. 
        """
        sql_get_primary = """
                            SELECT 
                                column_name, constraint_name
                            FROM 
                                all_ind_columns a, all_constraints b
                            WHERE 
                                a.index_owner = b.owner 
                            AND 
                                a.table_name = b.table_name
                            AND
                                a.index_name = b.index_name
                            AND
                                b.constraint_type = 'P'
                            AND 
                                b.owner = '{0}'
                            AND
                                b.table_name = '{1}'
                            ORDER BY 
                                a.column_position
                         """.format(self.owner, tablename)
        
        self.cursor.execute(sql_get_primary)
        primary_key_list = self.cursor.fetchall()
        
        # If partitioned table, the primary key needs to be added with the partitioning key
        if len(primary_key_list) > 0:
            if partitioned == 'YES':
                sql_get_partitionkey = """
                                        SELECT
                                            column_name
                                        FROM 
                                            all_part_key_columns
                                        WHERE 
                                            owner = '{0}'
                                      AND
                                            name = '{1}'
                                        ORDER BY 
                                            column_position
                                       """.format(self.owner, tablename)
                self.cursor.execute(sql_get_partitionkey)
                for part_key in self.cursor.fetchall():
                    if part_key[0] not in primary_key_list[:][0]:
                        primary_key_list.append((part_key[0], primary_key_list[0][1]))

            if primary_key_list[0][1].lower() in self.reservewords_list or len(primary_key_list[0][1].split()) > 1:
                constraint_name = '`' + primary_key_list[0][1].lower() + '`'
            else:
                constraint_name = primary_key_list[0][1].lower() if not re.search(r'^SYS\_', primary_key_list[0][1]) else tablename.lower() + '_pk'

            key_str = ''
            for key in primary_key_list :
                key_str = key_str + key[0].lower() + ','
            
            primary_sql = '    constraint ' + constraint_name + ' primary key(' + key_str.rstrip(',') + ')'
        else:
            primary_sql = None
        
        return primary_sql
    
    def transf_comment(self, tablename):     
        """
        Construct table comment
        """
        sql_get_tabcomment = """
                                SELECT 
                                    comments 
                                FROM 
                                    all_tab_comments  
                                WHERE
                                    owner = '{0}' 
                                AND 
                                    table_name = '{1}'
                             """.format(self.owner, tablename)
        
        self.cursor.execute(sql_get_tabcomment)
        table_comment_list = self.cursor.fetchone()

        if table_comment_list[0] is not None:
            if self.dbtype == 'tdsql':
                table_comment_sql = "  comment = '" + table_comment_list[0] + "'"
            elif self.dbtype == 'tbase':
                table_comment_sql = 'comment on table ' + tablename.lower() + " is '" + table_comment_list[0] + "';\n"
        else:
            table_comment_sql = None
        
        return table_comment_sql

    def transf_partition(self, tablename):
        """
        Transfer oracle partition to others
        """
        # Get partition type and key
        sql_get_parttype = """
                            SELECT 
                                partitioning_type 
                            FROM
                                all_part_tables 
                            WHERE
                                owner = '{0}'
                            AND 
                                table_name = '{1}'
                           """.format(self.owner, tablename)  
        
        sql_get_partkey = """
                            SELECT
                                column_name
                            FROM
                                all_part_key_columns
                            WHERE
                                owner = '{0}'
                            AND
                                name = '{1}'
                            ORDER BY column_position
                          """.format(self.owner, tablename)  
        
        self.cursor.execute(sql_get_parttype)
        parttype = self.cursor.fetchone()[0].lower()

        self.cursor.execute(sql_get_partkey)
        partkey_list = [i[0] for i in self.cursor.fetchall()]
        partkey = str(tuple(partkey_list)).lower().replace("'", '') if len(partkey_list) > 1 else str(tuple(partkey_list)).lower().replace(',','').replace("'", '')

        # Get all partitions infomation
        sql_get_partinfo = """
                            SELECT 
                                partition_name, high_value
                            FROM
                                all_tab_partitions
                            WHERE
                                table_owner = '{0}'
                            AND
                                table_name = '{1}'
                           """.format(self.owner, tablename)
            
        self.cursor.execute(sql_get_partinfo)
        partinfo_dict = {x[0]:x[1] for x in self.cursor.fetchall()}

        # handle to_date value
        for partiton_name, partition_value in partinfo_dict.items():
            if partition_value is not None:
                if re.search(r'^TO_DATE', partition_value):
                    pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
                    match = re.search(pattern, partition_value)
                    if match:
                        partinfo_dict[partiton_name] = "'" + match.group() + "'"
            
        if self.dbtype == 'tdsql':
            if parttype == 'hash':
                partition_sql = 'partition by hash' +  partkey + ' (\n'
                for partition_name in partinfo_dict.keys():
                    partition_sql = partition_sql + ' partition ' + partition_name.lower() + ',\n'
            elif parttype == 'list':
                partition_sql = 'partition by list columns' +  partkey + ' (\n' 
                for partition_name, partition_value in partinfo_dict.items():
                    partition_sql = partition_sql + ' partition '  + partition_name.lower() + " values in (" + partition_value + "),\n"
            elif parttype == 'range':
                partition_sql = 'partition by range columns ' + partkey + ' ( \n'
                for partition_name, partition_value in partinfo_dict.items():
                    partition_sql = partition_sql + ' partition ' + partition_name.lower() + " values less than (" + partition_value + "),\n"
                
            partition_sql = partition_sql.rstrip(',\n') + ')'      
        elif self.dbtype == 'tbase':
            partition_sql = 'partition by ' + parttype + partkey + '; \n\n'
            if parttype == 'hash':
                part_num = 0
                for partition_name, partition_value in partinfo_dict.items():
                    partition_sql = partition_sql + 'create table ' + tablename.lower() + '_' + partition_name.lower() + \
                                    ' partition of ' + tablename.lower()  + ' for values with (modules ' + \
                                    str(len(partinfo_dict)) + ',remainder ' + str(part_num) + ');\n'
                    part_num = part_num + 1
            elif parttype == 'list':
                for partition_name, partition_value in partinfo_dict.items():
                    partition_sql = partition_sql + 'create table ' + tablename.lower() + '_' + partition_name.lower() + \
                                    ' partition of ' + tablename.lower() + ' for values in (' + partition_value + ");\n"
            elif parttype == 'range':
                partition_value_prev = 'minvalue'
                for partition_name, partition_value in partinfo_dict.items():
                    partition_sql = partition_sql + 'create table ' + tablename.lower() + '_' + partition_name.lower() + \
                                    ' partition of ' + tablename.lower() + ' for values from (' + partition_value_prev + ') to (' + partition_value + ');\n'
                    partition_value_prev = partition_value
                partition_sql = partition_sql + 'create table ' + tablename.lower()  + '_def partition of ' + tablename.lower() + ' default;\n'
        
        return partition_sql

    def transf_constraint(self, tablename):
        """
        Transfer oracle constraint to others, exclude not null and foreign key.
        """
        sql_get_constraint = """
                                SELECT 
                                    constraint_name, constraint_type, search_condition_vc
                                FROM
                                    all_constraints
                                WHERE 
                                    search_condition_vc not like '%NOT NULL'
                                AND
                                    owner = '{0}'
                                AND
                                    table_name = '{1}'
                             """.format(self.owner, tablename)
        
        self.cursor.execute(sql_get_constraint)
        constraint_list = [ i for i in self.cursor.fetchall() ]

        constraint_sql = ''

        if len(constraint_list) > 0:
            for constraint in constraint_list:
                if constraint[0] in self.reservewords_list or len(constraint[0].split()) > 1:
                    constraint_name = '`' + constraint[0].lower() + '`'
                else:
                    constraint_name = constraint[0].lower()
                
                constraint_sql = constraint_sql + ('    constraint ' + constraint_name if not re.search(r'^sys\_', constraint_name) else '') + \
                                 '   check(' + constraint[2] + '),\n'
        
            constraint_sql = constraint_sql.rstrip(',\n')
        else:
            constraint_sql = None
        
        return constraint_sql

    def transf_single_index(self, tablename, partitioned):
        """
        transfer single table index
        """
        index_sql = None
        # get index info 
        sql_get_index = """
                            SELECT 
                                a.index_name, a.uniqueness, b.column_name, b.column_position
                            FROM 
                                all_indexes a, all_ind_columns b
                            WHERE
                                a.index_name = b.index_name
                                AND a.owner = b.index_owner
                                AND a.index_type = 'NORMAL'
                                AND a.owner = '{0}'
                                AND a.table_name = '{1}'
                                AND a.index_name NOT IN (
                                    SELECT /*+ unnest */
                                        index_name
                                    FROM
                                        all_constraints c
                                    WHERE 
                                        c.constraint_type = 'P'
                                        AND c.owner = '{0}'
                                        AND c.table_name = '{1}'
                                ) 
                            ORDER BY 1,4
                        """.format(self.owner, tablename)
        
        self.cursor.execute(sql_get_index)
        index_info_list = {}

        for key, group in itertools.groupby(self.cursor.fetchall(), key=lambda x: x[0]):
            index_info_list[key] = list(group)

        # no partitioned table
        if partitioned == 'NO':
            for index_info in index_info_list.values():
                for column_info in index_info:
                    column_position = column_info[3]
                    if column_position == 1:
                        index_sql = 'create' + (' unique ' if index_info[0][1] == 'UNIQUE' else ' ') +  'index ' + column_info[0].lower() + ' on ' + tablename.lower() + '(' + column_info[2].lower()
                    else:
                        index_sql = index_sql + ',' + column_info[2].lower()
                index_sql = index_sql + ');\n'
        elif partitioned == 'YES':
            sql_get_partkey = """
                                SELECT 
	                                column_name, column_position
                                FROM 
	                                all_part_key_columns
                                WHERE 
	                                owner = '{0}'
	                                AND name = '{1}'
                                    ORDER BY column_position
                              """.format(self.owner, tablename)
            
            self.cursor.execute(sql_get_partkey)
            partkey_list = self.cursor.fetchall() 

            for index_info in index_info_list.values():
                for column_info in index_info:
                    column_position = column_info[3]
                    if column_position == 1:
                        index_sql = 'create' + (' unique ' if column_info[1] == 'UNIQUE' else ' ') +  'index ' + column_info[0].lower() + ' on ' + tablename.lower() + '(' + column_info[2].lower()
                    else:
                        index_sql = index_sql + ',' + column_info[2].lower()
                
                # unique index add partkey, if need
                if index_info[0][1] == 'UNIQUE':
                    for partkey in partkey_list:
                        if partkey not in [key[2] for key in index_info]:
                            index_sql = index_sql + ',' + partkey[0].lower()

                index_sql = index_sql + ');\n'

        return index_sql

    def transf_index(self, options, transf_table_list):
        """
        Centralized processing of index conversion, input the results to a separate index file
        """
        # no partitioned table and partitioned uniqueness index
        part_table_list = tuple([i[1] for i in transf_table_list if i[2] == 'YES'])
        if len(part_table_list) == 0:
            part_table_list = "('')"

        if options is not None:
            table_list = [i[1] for i in transf_table_list]

        # all NONUNIQUE index
        sql_get_index = """
                            SELECT 
	                            a.table_name, a.index_name, a.uniqueness, b.column_name, b.column_position
                            FROM
	                            all_indexes a, all_ind_columns b
                            WHERE 
	                            a.index_name = b.index_name
	                            AND a.owner = b.index_owner
	                            AND a.index_type = 'NORMAL'
	                            AND a.owner = '{0}'
	                            AND a.uniqueness = 'NONUNIQUE'
                        """.format(self.owner) + \
                            ('AND a.TABLE_NAME IN {0}'.format(str(tuple(table_list)) if len(table_list) > 1 else str(tuple(table_list)).replace(',','')) if options is not None else '') + \
                            ' ORDER BY 1, 2, 5'
        
        self.cursor.execute(sql_get_index) 
        index_info_list = self.cursor.fetchall()
        
        # all nopartitioned table unique index, exclude primary index
        sql_get_index = """
                            SELECT 
	                            a.table_name, a.index_name, a.uniqueness, b.column_name, b.column_position
                            FROM
	                            all_indexes a, all_ind_columns b
                            WHERE 
	                            a.index_name = b.index_name
	                            AND a.owner = b.index_owner
	                            AND a.index_type = 'NORMAL'
	                            AND a.owner = '{0}'
	                            AND a.uniqueness = 'UNIQUE'
                                AND a.table_name IN (
                                    SELECT /*+ unnest */
                                        table_name
                                    FROM
                                        all_tables
                                    WHERE
                                        owner = '{0}'
                                        AND partitioned = 'NO')
								AND a.index_name NOT IN (
                                    SELECT /*+ unnest */ 
                                        index_name 
                                    FROM 
                                        all_constraints 
                                    WHERE 
                                        constraint_type = 'P' 
                                        AND owner = '{0}')
                                AND a.table_name NOT IN {1}
                                ORDER BY 1, 2, 5
                        """.format(self.owner, \
                        part_table_list if isinstance(part_table_list, str) else str(part_table_list) if len(part_table_list) > 1 else str(part_table_list).replace(',',''))

        self.cursor.execute(sql_get_index) 
        index_info_list = index_info_list + self.cursor.fetchall()
        
        # all partitioned table unique index, exclude primary key  
        for part_table in part_table_list:
            sql_get_index = """
                                SELECT
	                                index_name, table_name, 'UNIQUE', column_name, column_position
                                FROM
	                                all_ind_columns
                                WHERE
	                                table_name = '{1}'
	                                AND index_owner = '{0}'
	                                AND index_name IN (
	                                SELECT
		                                /*+ unnest */ index_name
                                	FROM
	                                	all_indexes
	                                WHERE
                                		uniqueness = 'UNIQUE'
                                		AND owner = '{0}'
                                		AND table_name = '{1}')
                                    AND index_name NOT IN (
                                    SELECT
		                                /*+ unnest */ index_name
	                                FROM
		                                all_constraints
	                                WHERE
		                                constraint_type = 'P'
		                                AND owner = '{0}'
		                                AND table_name = '{1}')
                                    ORDER BY 1, 2, 5
                            """.format(self.owner, part_table)
            
            self.cursor.execute(sql_get_index) 
            index_list = self.cursor.fetchall()

            sql_get_partkey = """
                                SELECT 
	                                column_name
                                FROM 
	                                all_part_key_columns
                                WHERE 
	                                owner = '{0}'
	                                AND name = '{1}'
                                    ORDER BY column_position
                              """.format(self.owner, part_table)
            
            self.cursor.execute(sql_get_partkey)
            partkey_list = self.cursor.fetchall() 
            
            # merge part key to unique index
            if len(index_list) > 0:
                index_list_bak = index_list.copy()
                for partkey in partkey_list:
                    index_position = 0
                    insert_position = 0
                    for index_info in index_list:
                        if index_info[4] == 1:
                            if index_position != 0:
                                if partkey not in column_list:
                                    index_list_bak.insert(insert_position,(index_name, index_info[1], index_info[2], partkey[0], len(column_list) + 1))
                                    insert_position = insert_position + 1
                            column_list = [index_info[3]]
                        else:
                            column_list.append(index_info[3])

                        index_position = index_position + 1
                        insert_position = insert_position + 1
                        index_name = index_info[0]
                
                    if partkey[0] not in column_list:
                        index_list_bak.append((index_name, index_info[1], index_info[2], partkey[0], len(column_list) + 1))
                
                    index_list = index_list_bak.copy()
            
                index_info_list = index_info_list + index_list_bak
        
        # Structure create index 
        index_position = 1
        index_sql = ''
        for index_info in index_info_list:
            if index_info[4] == 1:
                if index_position > 1:
                    index_sql = index_sql.rstrip(',') + ');\n'
                index_sql = index_sql + 'create ' + ('unique' if index_info[2] == 'UNIQUE' else '') +  ' index ' + index_info[0].lower() + ' on ' + index_info[1].lower() + '(' + index_info[3].lower()
            else:
                index_sql = index_sql + ',' + index_info[3].lower()
            index_position = index_position + 1
        
        index_sql = index_sql + ');\n'

        f = open(self.directory + '/' + self.owner.lower() + '_index.sql', 'wb')
        f.write(index_sql.encode('utf-8'))
        f.write('\n\n'.encode('utf-8'))
        f.close()
    
    def trans_single_ddl(self, tablename, partitioned):
        """
        Convert DDL statement of single table.
        """    
        column_sql, comment_sql = self.transf_column(tablename)
        primary_sql = self.transf_primary(tablename, partitioned)
        table_comment_sql = self.transf_comment(tablename)
        if partitioned == 'YES':
            partition_sql = self.transf_partition(tablename)
        constraint_sql = self.transf_constraint(tablename)
        
        ddl_str = 'create table ' + tablename.lower() + '(\n' + column_sql + ((',\n' + primary_sql) if primary_sql else '') + \
                  ((',\n' + constraint_sql) if constraint_sql else '') + ')\n'
        if self.dbtype == 'tdsql':
            ddl_str = ddl_str + '  engine = innodb\n' + ((table_comment_sql + '\n') if table_comment_sql else '') + \
                      (partition_sql if partitioned == 'YES' else '') + ';\n'
        elif self.dbtype == 'tbase':
            ddl_str = ddl_str + (partition_sql if partitioned == 'YES' else ';\n') + \
                      (comment_sql if comment_sql is not None else '') + \
                      (table_comment_sql if table_comment_sql is not None else '')

        return ddl_str
    
    def open(self):
        """
        connect_string:database connection
        build a connection of oracle
        """
        try:
            self.connect=cx_Oracle.connect(**source_conn)
            self.cursor=self.connect.cursor()
            self.status=True
        except:
            self.status=False
            print("can't open: ",source_conn)	  
     
    def close(self):
        """
        close the connection
        """
        if self.status:
            self.status=False
            try:
                self.cursor.close()
                self.connect.close()
            except:
                self.status=False

def export_all_ddl(**param):
    """
    Convert DDL statement of input table list
    """
    ora_param = {'dbtype' : param['dbtype'], \
                 'owner' : param['owner'], \
                 'directory' : param['directory'], \
                 'content' : param['content']}
    sourcedb = Trans_Ora_DDL(**ora_param)
    sourcedb.open()
    if param['mode'] == 'direct':
        if param['dbtype'] == 'tdsql':
            target_db = pymysql.connect(**target_conn)
        elif param['dbtype'] == 'tbase':
            target_db = psycopg2.connect(**target_conn)

        target_db.autocommit = True
        
        with target_db.cursor() as targetcur:
            for table in param['tablelist']:
                print("Processing table '{0}'.'{1}'...".format(param['owner'],table[1]))
                if param['content'] == 'table':
                    ddl_str = sourcedb.trans_single_ddl(table[1], table[2])
                elif param['content'] == 'index':
                    ddl_str = sourcedb.transf_single_index(table[1], table[2])
                elif param['content'] == 'all':
                    ddl_str = sourcedb.trans_single_ddl(table[1], table[2])
                    index_str = sourcedb.transf_single_index(table[1], table[2])
                    if index_str is not None :
                        ddl_str = ddl_str + index_str

                for ddl in ddl_str.split(';\n'):
                    if re.search(r'^create table', ddl):
                        if param['table_exists_action'] == 'drop':
                            targetcur.execute('drop table if exists {0}'.format(table[1]))
                            targetcur.execute(ddl)
                        elif param['table_exists_action'] == 'skip':
                            try:
                                targetcur.execute(ddl)
                            except Exception as err:
                                print("Skip table '{0}'.'{1}'. Error: {2}".format(param['owner'], table[1], str(err)))
                                print(ddl)
                                break
                    elif re.search(r'^$', ddl):
                        continue
                    else:
                        targetcur.execute(ddl)               
    elif param['mode'] == 'file':
        f = open(param['directory'] + '/' + param['owner'].lower() + str(param['parallel']) + '.sql', 'wb')
    
        for table in param['tablelist']:
            ddl_str = sourcedb.trans_single_ddl(table[1], table[2])
            f.write(ddl_str.encode('utf-8'))
            f.write('\n\n'.encode('utf-8'))
    
        f.close()

    sourcedb.close()
    target_db.close()

def parallel_export(**param):
    """
    Convert DDL statement of all table
    """
    # get table list
    ora_param = {'dbtype' : param['dbtype'], \
                 'owner' : param['owner'], \
                 'directory' : param['directory'], \
                 'content' : param['content']}
    ora = Trans_Ora_DDL(**ora_param)
    ora.open()
    transf_table_list = ora.get_table_list(param['options'], param['tablefile'])
    if param['mode'] == 'file' and param['content'] in ('all', 'index'):
        ora.transf_index(param['options'], transf_table_list)
    else:
        threads = []

        export_all_ddl_param = {'dbtype' : param['dbtype'], \
                                'owner' : param['owner'], \
                                'mode' : param['mode'], \
                                'content' : param['content'], \
                                'table_exists_action' : param['table_exists_action'], \
                                'directory' : param['directory']}

        for thread_num in range(param['parallel']):
            export_all_ddl_param['parallel_num'] = thread_num + 1
            if thread_num == param['parallel'] - 1:
                export_all_ddl_param['tablelist'] = transf_table_list[int(thread_num*len(transf_table_list)/param['parallel']):]
            else:
                export_all_ddl_param['tablelist'] = transf_table_list[int(thread_num*len(transf_table_list)/param['parallel']):int((thread_num+1)*len(transf_table_list)/param['parallel'])]
            
            thread = threading.Thread(target=export_all_ddl, kwargs=export_all_ddl_param)
        
            threads.append(thread)
            thread.start()
    
        for thread in threads:
            thread.join()
    
    ora.close()
    
def command_parse():
    """
    parse command and config file
    """

    parameter_dict = {'setting':{}, 'source':{}, 'target':{}}
    command_desc = "o2t is a tool for converting Oracle database metadata to tdsql or tbase."
    usage = "Usage: o2t.py -c o2t.ini"

    arvg_parser = argparse.ArgumentParser(usage=usage, description=command_desc,formatter_class=argparse.RawDescriptionHelpFormatter)

    arvg_parser.add_argument('-c', '--config', help="configure file, default ./o2t.ini")

    argv_namespace = arvg_parser.parse_args()

    if argv_namespace.config : 
        if os.path.exists(argv_namespace.config):
            configfile = argv_namespace.config
        else:
            print('Config file {0} is not exists!'.format(argv_namespace.config))
            sys.exit()
    else:
        if os.path.exists('./o2t.ini'):
            configfile = './o2t.ini'
        else:
            print('Config file ./o2t.ini is not exists!')
            sys.exit()
 
    # parse configure file
    arvg_config = configparser.ConfigParser()
    arvg_config.read(configfile)

    if 'source' in arvg_config.sections():
        source = arvg_config['source']
        if source.get('dbtype', 'oracle').lower() != 'oracle' :
            print('source dbtype only oracle!')
            sys.exit()
        else:
            parameter_dict['source']['dbtype'] = source.get('dbtype', 'oracle').lower()
        
        if source.get('host'):
            parameter_dict['source']['host'] = source.get('host')
        else:
            print('Missing source host!')
            sys.exit()
        
        parameter_dict['source']['port'] = source.get('port', 1521)

        if source.get('dbname'):
            parameter_dict['source']['dbname'] = source.get('dbname').lower()
        else:
            print('Missing source dbname!')
            sys.exit()
        
        if source.get('username'):
            parameter_dict['source']['username'] = source.get('username').lower()
        else:
            print('Missing source username!')
            sys.exit()
        
        if source.get('password'):
            parameter_dict['source']['password'] = source.get('password')
        else:
            if parameter_dict['source']['username'] == 'sys':
                parameter_dict['source']['password'] = None 
            else:
                print('Missing source password!')
                sys.exit()
    else:
        print('Config file {0} error, no source'.format(configfile))
        sys.exit()

    if 'setting' in arvg_config.sections():
        setting = arvg_config['setting']
        if setting.get('mode', 'direct').lower() not in ('file', 'direct'):
            print('mode only file or direct!')
            sys.exit()
        else:
            parameter_dict['setting']['mode'] = setting.get('mode', 'direct').lower()
        
        if setting.get('content', 'all').lower() not in ('table', 'index', 'all'):
            print('content only table, index or all!')
            sys.exit()
        else:
            parameter_dict['setting']['content'] = setting.get('content', 'all').lower()
        
        if not re.match(r'^[1-9][0-9]*$', setting.get('parallel', 1)):
            print('parallel only 1~8!')
            sys.exit()
        else:
            parameter_dict['setting']['parallel'] = int(setting.get('parallel', 1)) if int(setting.get('parallel', 1)) < 9 else 8

        if parameter_dict['source']['username'] == 'sys':
            if not setting.get('schema'):
                print('source username is sys, please input schema!')
                sys.exit()
            else:
                parameter_dict['setting']['schema'] = setting.get('schema').upper()
        else:
            parameter_dict['setting']['schema'] = setting.get('schema', parameter_dict['source']['username']).upper()
        
        if setting.get('table_exists_action', 'skip').lower() not in ('drop', 'skip'):
            print('table_exists_action only drop or skip!')
            sys.exit()
        else:
            parameter_dict['setting']['table_exists_action'] = setting.get('table_exists_action', 'skip')

        parameter_dict['setting']['include'] = setting.get('include')
        parameter_dict['setting']['exclude'] = setting.get('exclude')
        parameter_dict['setting']['directory'] = setting.get('directory', parameter_dict['setting']['schema'])
    else:
        parameter_dict['setting']['mode'] = 'direct'
        parameter_dict['setting']['content'] = 'all'
        parameter_dict['setting']['parallel'] = 1
        parameter_dict['setting']['include'] = None 
        parameter_dict['setting']['exclude'] = None
        parameter_dict['setting']['table_exists_action'] = 'skip'
        
        if parameter_dict['source']['username'] == 'sys':
            print('source username is sys, please input schema!')
            sys.exit()
        else:
            parameter_dict['setting']['schema'] = parameter_dict['source']['username']

        parameter_dict['setting']['directory'] = parameter_dict['setting']['schema']

    if 'target' in arvg_config.sections():
        target =   arvg_config['target']
        if target.get('dbtype', 'tdsql').lower() not in ('tdsql', 'tbase') :
            print('target dbtype only tdsql or tbase!')
            sys.exit()
        else:
            parameter_dict['target']['dbtype'] = target.get('dbtype', 'tdsql').lower()
            
        if target.get('host'):
            parameter_dict['target']['host'] = target.get('host')
        else:
            print('Missing target host!')
            sys.exit()
            
        parameter_dict['target']['port'] = int(target.get('port', 3306))

        if target.get('dbname'):
            parameter_dict['target']['dbname'] = target.get('dbname').lower()
        else:
            print('Missing target dbname!')
            sys.exit()

        if target.get('username'):
            parameter_dict['target']['username'] = target.get('username').lower()
        else:
            print('Missing target username!')
            sys.exit()
            
        if target.get('password'):
            parameter_dict['target']['password'] = target.get('password')
        else:
            print('Missing target password!')
            sys.exit()
    else:
        print('Config file {0} error, no target'.format(configfile))
        sys.exit()

    
    # check whether the table file format is correct
    if parameter_dict['setting']['include'] is not None and parameter_dict['setting']['exclude'] is not None:
        print('include and exclude cannot appear at the same time!')
        sys.exit()

    if parameter_dict['setting']['include'] is not None or parameter_dict['setting']['exclude'] is not None:
        table_file = parameter_dict['setting']['include'] if parameter_dict['setting']['include'] else parameter_dict['setting']['exclude']
        if os.path.exists(table_file):
            with open(table_file, 'r') as f:
                for line in f:
                    if not re.search(r'^#', line):
                        if len(line.split()) != 1:
                            print('The format of the file ' + table_file + ' is incorrect!')
                            print('One table name per row')
                            sys.exit()
        else:
            print(table_file + ' is not exists')
            sys.exit()
    
    return parameter_dict

if __name__ == '__main__':
    title = "\nO2t:  Version 1.0 From chenxw 2022 - Production on" + datetime.datetime.now().strftime('%a %b %d %H:%M:%S %Y')
    print(title + '\n')
    command_line_args = command_parse()

    global source_conn
    source_conn = {'user':command_line_args['source']['username'], \
                   'password':command_line_args['source']['password'], \
                   'dsn':command_line_args['source']['host'] + ':' + command_line_args['source']['port'] + '/' + command_line_args['source']['dbname']}
    if source_conn['user'] == 'sys':
        source_conn['mode'] = 'cx_Oracle.SYSDBA'

    if command_line_args['setting']['mode'] == 'direct':
        global target_conn
        target_conn = {'user':command_line_args['target']['username'], \
                       'password':command_line_args['target']['password'], \
                       'host':command_line_args['target']['host'], \
                       'port':command_line_args['target']['port'], \
                       'database':command_line_args['target']['dbname']}
    
    try:
        sourcedb = cx_Oracle.connect(**source_conn)
        with sourcedb.cursor() as sourcecur:
            sourcecur.execute('select banner from v$version')
            version_info = sourcecur.fetchone()
            print('Connected to source: ' + version_info[0])
        sourcedb.close()
    except Exception as err:
        print('Connected to source {0} failed!'.format(str(source_conn)))
        print(err)
        sys.exit()
    
    if command_line_args['setting']['mode'] == 'direct':
        try:
            targetdb = pymysql.connect(**target_conn) if command_line_args['target']['dbtype'] == 'tdsql' else psycopg2.connect(**target_conn)
            with targetdb.cursor() as targetcur:
                targetcur.execute('select version()')
                version_info = targetcur.fetchone()
                print('Connected to target: ' + version_info[0])
            targetdb.close()
        except Exception as err:
            print('Connected to target {0} {1} failed!'.format(command_line_args['target']['dbtype'], str(target_conn)))
            print(err)
            sys.exit()
    elif command_line_args['setting']['mode'] =='file':
        if not os.path.exists(command_line_args['setting']['directory']):
            os.makedirs(command_line_args['setting']['directory'])
    
        if len(os.listdir(command_line_args['setting']['directory'])) > 0:
            print('Directory {0} is not empty!'.format(command_line_args['setting']['directory']))
            sys.exit()

    task_info = '\nStarting transfer ' + command_line_args['source']['host'] + ' schema {0}'.format(command_line_args['setting']['content'] if command_line_args['setting']['content'] != 'all' else 'table and index') + ' to ' 

    if command_line_args['setting']['mode'] == 'file':
        task_info = task_info + ' file {0}'.format(command_line_args['setting']['directory'])
    else:
        task_info = task_info + command_line_args['target']['host'] + ' dbname ' + command_line_args['target']['dbname'] 

    print(task_info)

    parallel_export_param = {'parallel' : command_line_args['setting']['parallel'], \
            'dbtype' : command_line_args['target']['dbtype'], \
            'owner' : command_line_args['setting']['schema'], \
            'mode' : command_line_args['setting']['mode'], \
            'content' : command_line_args['setting']['content'], \
            'table_exists_action' : command_line_args['setting']['table_exists_action'], \
            'directory' : command_line_args['setting']['directory']}

    if command_line_args['setting']['include'] is not None:
        parallel_export_param['tablefile'] = command_line_args['setting']['include']
        parallel_export_param['options'] = 'include'
    elif command_line_args['setting']['exclude'] is not None:
        parallel_export_param['tablefile'] = command_line_args['setting']['exclude']
        parallel_export_param['options'] = 'exclude'
    else:
        parallel_export_param['tablefile'] = None 
        parallel_export_param['options'] = None
    
    try:
        parallel_export(**parallel_export_param)
    except Exception as err:
        print(err)
        sys.exit()
    
    print('Transfer job successfully completed at ' + datetime.datetime.now().strftime('%a %b %d %H:%M:%S %Y'))