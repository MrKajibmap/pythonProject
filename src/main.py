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
    clmnFullTmplt = {'mapping': [], 'tables': [], 'librefs': []}
    clmnSimpleTmplt = {
        "serial": "",
        "target": "",
        "length": "",
        "type": "",
        "label": "",
        "bk": ""
    }
    clmnFull = copy.deepcopy(clmnFullTmplt)
    clmnSimple = copy.deepcopy(clmnSimpleTmplt)
    clmnsGeneral = {}
    with open('./src/files/input/101_138_02_Extract_DGP_DWH_TRN_TYPE.sas', 'r', encoding='utf-16') as deploy:
        for line in deploy:
            # initial new dict
            # job_nm
            if re.search(dict_job_nm[0], line):
                print(line.split(sep='=')[-1].split(sep='%nrquote(')[-1].split(sep=');')[0].strip())
            # libnames
            if re.search(dict_libname[0], line):
                for j in range(0, line.count(' ') + 1):
                    if len(line.split(sep=';')[0].split(sep=' ')[j].strip()) > 0:
                        libraries.append(line.split(sep=' ')[j].split(sep=';')[0].strip())
                        print(line.split(sep=' ')[j].split(sep=';')[0].strip())
            # input table
            if re.search(dict_input_table[0], line):
                print('input_table_name = ', line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[-1].strip())
            # output table
            if re.search(dict_output_table[0], line):
                print('output_table_name = ', line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[-1].strip())
            # колонки таблицы источника и таблицы _FULL
            if re.search(dict_of_key_words_mapping[0], line):
                # clmnFull['mapping'].append({'name' : line.split(sep='=')[-1].split(sep=';')[0].strip()})
                clmnSimple['target'] = line.split(sep='=')[-1].split(sep=';')[0].strip()
            if re.search('\Wlet _OUTPUT_col\d{1,}_length =', line):
                # clmnFull['length'] = line.split(sep='=')[-1].strip()
                # clmnFull['mapping'].append({'length': line.split(sep='=')[-1].strip()})
                clmnSimple['length'] = line.split(sep='=')[-1].strip()
                print(clmnSimple)
            if re.search('\Wlet _OUTPUT_col\d{1,}_type', line):
                clmnSimple['type'] = line.split(sep='=')[-1].split(sep=';')[0].strip()
                # clmnFull['mapping'].append({'type': line.split(sep='=')[-1].split(sep=';')[0].strip()})
                print(clmnSimple)
                # clmnFull['tables'].append(
                #     {'test_privet': line.split(sep='=')[-1].split(sep=')')[0]})
            if re.search('\Wlet _OUTPUT_col\d{1,}_label =', line):
                clmnSimple['label'] = line.split(sep='=')[-1].split(sep=')')[0].split(sep='(')[-1].strip()
                print(clmnSimple)
                clmnToAppend = copy.deepcopy(clmnSimple)
                # clmnFull['mapping'].append({'label': line.split(sep='=')[-1].split(sep=')')[0].split(sep='(')[-1].strip()})
                # clmnFull['tables'].append(
                #     {'test': line.split(sep='=')[-1].split(sep=')')[0].split(sep='(')[-1].strip()})
                clmnFull['mapping'].append(clmnToAppend)
                print('>>>>>>', clmnFull)
                with open('./src/files/output/tst.json', 'w') as jsonf:
                    # data = json.dumps(clmnFull)
                    # data = json.loads(str(data))
                    # json.dump(json.loads(json.dumps(clmnFull)), jsonf, indent=4, sort_keys=True)
                    json.dump(clmnFull, jsonf, indent=4, sort_keys=True)
                    jsonf.close()

    print(list(libraries))
