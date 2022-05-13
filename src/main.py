import copy
import json
import re

if __name__ == '__main__':
    dict_of_key_words = ['etl_job_start', 'etl_job_finish']
    dict_of_key_words_mapping = ['\Wlet _OUTPUT_col\d{1,}_name']
    dict_job_nm = ['etls_jobName']
    dict_libname = ['LIBNAME']
    dict_input_table = ['_INPUT =']
    dict_output_table = ['_OUTPUT =']
    libraries = []
    taskExtractTmplt = {
        "tasks": [
            {
                "task_id": "",
                "description": "",
                "type": "",
                "source": [{
                    "connection": "",
                    "path": "",
                    "entity": "",
                    "alias": ""
                }],
                "target": [
                    {
                        "connection": "",
                        "path": "",
                        "entity": "",
                        "alias": ""
                    }],
                "mapping": []
            }]
    }
    taskSinglePtrn = {
                "task_id": "",
                "description": "",
                "type": "",
                "source": [{
                    "connection": "",
                    "path": "",
                    "entity": "",
                    "alias": ""
                }],
                "target": [
                    {
                        "connection": "",
                        "path": "",
                        "entity": "",
                        "alias": ""
                    }],
                "mapping": []
            }
    clmnFullTmplt = {'mapping': [], 'tables': [], 'librefs': []}
    clmnSimpleTmplt = {
        "serial": "",
        "target": "",
        "expression": "",
        "type": "",
        "label": "",
        "bk": "",
        "source": [{
            "serial": "1",
            "alias": "*_src",
            "column": "id"
        }]
    }
    clmnFull = copy.deepcopy(clmnFullTmplt)
    clmnSimple = copy.deepcopy(clmnSimpleTmplt)
    taskSimple = copy.deepcopy(taskExtractTmplt)
    taskSimple['tasks'][0]['description'] = 'extract '
    taskSimple['tasks'][0]['type'] = 'stg_oracle_odbc '
    taskSimple['tasks'][0]['task_id'] = '1'
    with open('./src/files/input/101_138_02_Extract_DGP_DWH_TRN_TYPE.sas', 'r', encoding='utf-16') as deploy:
        for line in deploy:
            # initial new dict
            # job_nm
            if re.search(dict_job_nm[0], line):
                print(line.split(sep='=')[-1].split(sep='%nrquote(')[-1].split(sep=');')[0].strip())
            # libnames
            if re.search(dict_libname[0], line):
                # newList = line.split()
                print(">>>> size of : \"", line.split(';')[0].strip().replace('  ', ' '), '" = ', len(line.strip().replace('  ', ' ').split(sep=' ')))
                match line.split(';')[0].split():
                    case operator, libref, type_conn, path, schema, authdomain:
                        print(f'{operator=}, {libref=}, {type_conn=}, {path=}')
                        taskSimple['tasks'][0]['source'][0]['connection'] = libref
                        taskSimple['tasks'][0]['source'][0]['path'] = path.split('"')[1]
                        taskSimple['tasks'][0]['source'][0]['entity'] = type_conn
                        taskSimple['tasks'][0]['source'][0]['alias'] = libref+'_src'
                    case operator, libref, type_conn, path:
                        print(f'{operator=}, {libref=}, {type_conn=}, {path=}')
                        taskSimple['tasks'][0]['target'][0]['connection'] = libref
                        taskSimple['tasks'][0]['target'][0]['path'] = path.split('"')[1]
                        taskSimple['tasks'][0]['target'][0]['entity'] = type_conn
                        taskSimple['tasks'][0]['target'][0]['alias'] = libref
                    case _:
                        print('wrong libname operator definition')
                for j in range(0, line.count(' ') + 1):
                    if len(line.split(sep=';')[0].split(sep=' ')[j].strip()) > 0:
                        libraries.append(line.split(sep=' ')[j].split(sep=';')[0].strip())
                        print(line.split(sep=' ')[j].split(sep=';')[0].strip())
                # 3 element from each libname is path
            # input table
            if re.search(dict_input_table[0], line):
                print('input_table_name = ', line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[-1].strip())
            # output table
            if re.search(dict_output_table[0], line):
                print('output_table_name = ', line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[-1].strip())
            # колонки таблицы источника и таблицы _FULL
            if re.search(dict_of_key_words_mapping[0], line):
                clmnSimple['target'] = line.split(sep='=')[-1].split(sep=';')[0].strip()
                clmnSimple['expression'] = line.split(sep='=')[-1].split(sep=';')[0].strip()
                clmnSimple['serial'] = str(int(line.split(sep='col')[-1].split(sep='_')[0].strip()) + 1)
                clmnSimple['source'][0]['serial'] = str(int(line.split(sep='col')[-1].split(sep='_')[0].strip()) + 1)
                clmnSimple['source'][0]['alias'] = clmnSimple['target'] + '_src'
            if re.search('\Wlet _OUTPUT_col\d{1,}_length =', line):
                clmnSimple['length'] = line.split(sep='=')[-1].split(sep=';')[0].strip()
            if re.search('\Wlet _OUTPUT_col\d{1,}_type', line):
                if line.split(sep='=')[-1].split(sep=';')[0].strip() == '$':
                    clmnSimple['type'] = 'char' + '(' + clmnSimple.get('length') + ')'
                else:
                    clmnSimple['type'] = 'numeric'
            # Доходим до конца
            if re.search('\Wlet _OUTPUT_col\d{1,}_label =', line):
                clmnSimple['label'] = line.split(sep='=')[-1].split(sep=')')[0].split(sep='(')[-1].strip()
                # delete extra fields not used in future
                del clmnSimple['length']
                # dummy for bk-key = 'false'
                clmnSimple['bk'] = 'false'
                clmnToAppend = copy.deepcopy(clmnSimple)
                clmnFull['mapping'].append(clmnToAppend)
                taskSimple['tasks'][0]['mapping'].append(clmnToAppend)
                with open('./src/files/output/tst.json', 'w') as jsonf:
                    # json.dump(json.loads(json.dumps(clmnFull)), jsonf, indent=4, sort_keys=True)
                    json.dump(taskSimple, jsonf, indent=4)
                    jsonf.close()
