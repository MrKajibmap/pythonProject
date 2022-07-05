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
    dict_transform_flag = ['Transform']
    dict_transform_stop_list = ['Начать модуль', 'Завершить модуль']
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
    clmnFullTmplt = {'mapping': [], 'tables': [], 'librefs': []}
    clmnSimpleTmplt = {
        "serial": "",
        "target": "",
        "expression": "",
        "type": "",
        "label": "",
        "bk": "",
        "source": [{
            "serial": "",
            "alias": "",
            "column": ""
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
    clmnFull = copy.deepcopy(clmnFullTmplt)
    clmnSimple = copy.deepcopy(clmnSimpleTmplt)
    taskSimple = copy.deepcopy(taskExtractTmplt)
    print('=1234', taskSimple['tasks'][0])
    taskFull = copy.deepcopy(taskExtractTmplt)
    TaskCnt = -1
    taskDesc = 'temp'
    # with open('./files/input/101_16_92_Extract_Mis_VDMTB_MONTH.sas', 'r', encoding='utf-8') as deploy:
    with open('./files/input/101_75_15_Extract_CAS_CASIOBOP.sas', 'r', encoding='utf-8') as deploy:
        for line in deploy:
            # calc tasks count
            #  contains a transform mask
            if re.search(dict_transform_flag[0], line):
                # not in stop list
                if line.replace('*', '').split(':')[1].strip() not in dict_transform_stop_list:
                    TaskCnt += 1
                    taskDesc = line.replace('*', '').split(':')[1].strip()
                    print('We have searched new task id, current task_id =====', TaskCnt, ' the name of task is = ', taskDesc)
                    # add new template for new task
                    if TaskCnt > 0:
                        print('New Task with ID =', TaskCnt, 'was added')
                        taskSimple['tasks'].append(copy.deepcopy(taskSinglePtrn))
                        print(taskSimple)
                    taskSimple['tasks'][TaskCnt]['task_id'] = str(TaskCnt)
                    taskSimple['tasks'][TaskCnt]['description'] = taskDesc
            # job_nm
            if re.search(dict_job_nm[0], line):
                print('Job Name: ', line.split(sep='=')[-1].split(sep='%nrquote(')[-1].split(sep=');')[0].strip())
            # libnames
            if re.search(dict_libname[0], line):
                print('New line for libname: ', line)
                match line.split(';')[0].split():
                    case operator, libref, type_conn, path, schema, authdomain:
                        print(f'{operator=}, {libref=}, {type_conn=}, {path=}')
                        taskSimple['tasks'][TaskCnt]['source'][0]['connection'] = libref
                        taskSimple['tasks'][TaskCnt]['source'][0]['path'] = path.split('"')[1]
                        # taskSimple['tasks'][TaskCnt]['source'][0]['entity'] = type_conn
                        taskSimple['tasks'][TaskCnt]['source'][0]['alias'] = libref + '_src'
                    case operator, libref, type_conn, defer_opt, path, schema, authdomain:
                        print(f'{operator=}, {libref=}, {type_conn=}, {path=}')
                        taskSimple['tasks'][TaskCnt]['source'][0]['connection'] = libref
                        taskSimple['tasks'][TaskCnt]['source'][0]['path'] = path.split('"')[1]
                        # taskSimple['tasks'][TaskCnt]['source'][0]['entity'] = type_conn
                        taskSimple['tasks'][TaskCnt]['source'][0]['alias'] = libref + '_src'
                    case operator, libref, type_conn, path:
                        print(f'{operator=}, {libref=}, {type_conn=}, {path=}')
                        taskSimple['tasks'][0]['target'][0]['connection'] = libref
                        taskSimple['tasks'][0]['target'][0]['path'] = path.split('"')[1]
                        # taskSimple['tasks'][0]['target'][0]['entity'] = type_conn
                        taskSimple['tasks'][0]['target'][0]['alias'] = libref
                    case operator, libref, type_conn, defer_opt, readbuff_opt, datasrc_opt, schema, authdomain:
                        print(f'{operator=}, {libref=}, {type_conn=}, {datasrc_opt=}')
                        taskSimple['tasks'][TaskCnt]['source'][0]['connection'] = libref
                        taskSimple['tasks'][TaskCnt]['source'][0]['path'] = datasrc_opt.split('=')[1]
                        # taskSimple['tasks'][TaskCnt]['source'][0]['entity'] = type_conn
                        taskSimple['tasks'][TaskCnt]['source'][0]['alias'] = libref + '_src'
                    case _:
                        print('wrong libname operator definition')
            # input table
            if re.search(dict_input_table[0], line):
                print('input_table_name = ', line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[-1].strip())
                taskSimple['tasks'][TaskCnt]['source'][0]['entity'] = line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[-1].strip()
                taskSimple['tasks'][TaskCnt]['source'][0]['connection'] = line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[0].strip()
            # output table
            if re.search(dict_output_table[0], line):
                print('output_table_name = ', line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[-1].strip())
                taskSimple['tasks'][TaskCnt]['target'][0]['entity'] = \
                line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[-1].strip()
                taskSimple['tasks'][TaskCnt]['target'][0]['connection'] = line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[0].strip()
                # колонки таблицы источника и таблицы _FULL
            if re.search(dict_of_key_words_mapping[0], line):
                clmnSimple['target'] = line.split(sep='=')[-1].split(sep=';')[0].strip()
                clmnSimple['expression'] = line.split(sep='=')[-1].split(sep=';')[0].strip()
                clmnSimple['serial'] = str(int(line.split(sep='col')[-1].split(sep='_')[0].strip()) + 1)
                clmnSimple['source'][0]['serial'] = str(int(line.split(sep='col')[-1].split(sep='_')[0].strip()) + 1)
                clmnSimple['source'][0]['alias'] = clmnSimple['target'] + '_src'
                clmnSimple['source'][0]['column'] = clmnSimple['target']
            if re.search('\Wlet _OUTPUT_col\d{1,}_length =', line):
                clmnSimple['length'] = line.split(sep='=')[-1].split(sep=';')[0].strip()
            if re.search('\Wlet _OUTPUT_col\d{1,}_type', line):
                if line.split(sep='=')[-1].split(sep=';')[0].strip() == '$':
                    clmnSimple['type'] = 'char' + '(' + clmnSimple.get('length') + ')'
                else:
                    clmnSimple['type'] = 'numeric'
            # last row in deploy (label statement) for column definition
            if re.search('\Wlet _OUTPUT_col\d{1,}_label =', line):
                clmnSimple['label'] = line.split(sep='=')[-1].split(sep=')')[0].split(sep='(')[-1].strip()
                # delete extra fields not used in future
                del clmnSimple['length']
                # dummy for bk-key = 'false'
                clmnSimple['bk'] = 'false'
                clmnToAppend = copy.deepcopy(clmnSimple)
                clmnFull['mapping'].append(clmnToAppend)
                print('TaskCnt =', TaskCnt)
                taskSimple['tasks'][TaskCnt]['mapping'].append(clmnToAppend)
                print('Current state for taskSimple is: ', taskSimple)
                # taskFull['tasks'].append(taskSimple)
                with open('./files/output/tst_multiple.json', 'w') as jsonf:
                    # json.dump(json.loads(json.dumps(clmnFull)), jsonf, indent=4, sort_keys=True)
                    json.dump(taskSimple, jsonf, indent=4)
                    jsonf.close()
            # if taskDesc = Разместить выгрузку в архиве, then copy previous step mapping for this
            if taskDesc == 'Разместить выгрузку в архиве':
                if re.search('\WStep end '+taskDesc, line):
                    print('this is the endpoint of the step, which taskId was = ', TaskCnt)
                    # del taskSimple['tasks'][TaskCnt]
                    taskSimple['tasks'][TaskCnt]['mapping'].append(copy.deepcopy(taskSimple['tasks'][TaskCnt-1]['mapping']))
                    print(taskSimple)
                    with open('./files/output/tst_multiple.json', 'w') as jsonf:
                        # json.dump(json.loads(json.dumps(clmnFull)), jsonf, indent=4, sort_keys=True)
                        json.dump(taskSimple, jsonf, indent=4)
                        jsonf.close()
    print('TasksIdsCount >>>>  ', TaskCnt)
