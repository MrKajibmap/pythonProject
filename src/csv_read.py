import csv

import copy
import json
import re

# FLOWNAME;JOBNAME;NONSTANDART_JOB;TRANSFORMNAME;TRANSFORMORDER;SOURCETABLENAMEFULL;
# SOURCECOLUMNNAMEFULL;SRC_CLMN_TYPE;SRC_CLMN_LENGHT;TARGETTABLENAMEFULL;TARGETCOLUMNNAMEFULL;
# TRG_CLMN_TYPE;TRG_CLMN_LENGHT;COLUMNTRANSFORM;STOREDTEXT
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
    taskFull = copy.deepcopy(taskExtractTmplt)
    transformCnt = 0
    jobCnt = 0
    clmnCnt = 0
    taskDesc = 'temp'
    prev_transform = ''
    prev_job = ''
    with open('./files/input/flow_mapping_semic.csv', newline='', encoding='utf-8') as mapping:
        reader = csv.DictReader(mapping, delimiter=';')
        line_iter = 0
        for line in reader:
            line_iter = line_iter + 1
            current_transform = line['TRANSFORMNAME']
            current_job = line['JOBNAME']
            if current_job != prev_job:
                jobCnt = jobCnt + 1
                transformCnt = 0
                clmnCnt = 0
                # if line_iter > 1:
            # new transform -> require to add new task to json
            if current_transform != prev_transform:
                transformCnt = transformCnt + 1
                clmnCnt = 0
                # print('New Task with ID =', transformCnt, 'was added')
                taskSimple['tasks'].append(copy.deepcopy(taskSinglePtrn))
                taskSimple['tasks'][transformCnt - 1]['task_id'] = str(transformCnt)
                taskSimple['tasks'][transformCnt - 1]['description'] = line['TRANSFORMNAME']
                if line['NONSTANDART_JOB'] == '1':
                    taskSimple['tasks'][transformCnt - 1]['type'] = 'NONSTANDART'
                else:
                    taskSimple['tasks'][transformCnt - 1]['type'] = 'STANDART'
                if len(line['SOURCETABLENAMEFULL'].split(sep='.')) == 2:
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['connection'] = line['SOURCETABLENAMEFULL'].split(sep='.')[0]
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['path'] = line['SOURCETABLENAMEFULL'].split(sep='.')[0]
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['entity'] = line['SOURCETABLENAMEFULL'].split(sep='.')[1]
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['alias'] = line['SOURCETABLENAMEFULL'].split(sep='.')[1] + '_src'
                elif len(line['SOURCETABLENAMEFULL'].split(sep='.')) == 1:
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['connection'] = 'NONE'
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['path'] = 'NONE'
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['entity'] = line['SOURCETABLENAMEFULL'].split(sep='.')[0]
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['alias'] = line['SOURCETABLENAMEFULL'].split(sep='.')[0] + '_src'
                else:
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['connection'] = 'INVALID VALUE'
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['path'] = 'INVALID VALUE'
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['entity'] = 'INVALID VALUE'
                    taskSimple['tasks'][transformCnt - 1]['source'][0]['alias'] = 'INVALID VALUE'
                if len(line['TARGETTABLENAMEFULL'].split(sep='.')) == 2:
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['connection'] = line['TARGETTABLENAMEFULL'].split(sep='.')[0]
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['path'] = line['TARGETTABLENAMEFULL'].split(sep='.')[0]
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['entity'] = line['TARGETTABLENAMEFULL'].split(sep='.')[1]
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['alias'] = line['TARGETTABLENAMEFULL'].split(sep='.')[1] + '_src'
                elif len(line['TARGETTABLENAMEFULL'].split(sep='.')) == 1:
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['connection'] = 'NONE'
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['path'] = 'NONE'
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['entity'] = line['TARGETTABLENAMEFULL'].split(sep='.')[0]
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['alias'] =  line['TARGETTABLENAMEFULL'].split(sep='.')[0
                                                                                   ] + '_src'
                else:
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['connection'] = 'INVALID VALUE'
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['path'] = 'INVALID VALUE'
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['entity'] = 'INVALID VALUE'
                    taskSimple['tasks'][transformCnt - 1]['target'][0]['alias'] = 'INVALID VALUE'

                # print(taskSimple)
            # new clmn -> need to add new clmn to mapping
            # write the column
            # srcTableName
            clmnCnt += 1
            clmnSimple['serial'] = str(clmnCnt)
            if len(line['TARGETCOLUMNNAMEFULL'].split(sep='.')) == 3:
                clmnSimple['target'] = line['TARGETCOLUMNNAMEFULL'].split(sep='.')[2]
                clmnSimple['label'] = line['TARGETCOLUMNNAMEFULL'].split(sep='.')[2]
            elif len(line['TARGETCOLUMNNAMEFULL'].split(sep='.')) == 2:
                clmnSimple['target'] = line['TARGETCOLUMNNAMEFULL'].split(sep='.')[1]
                clmnSimple['label'] = line['TARGETCOLUMNNAMEFULL'].split(sep='.')[1]
            else:
                clmnSimple['target'] = 'INVALID VALUE'
                clmnSimple['label'] = 'INVALID VALUE'
            clmnSimple['expression'] = 'NONE'
            clmnSimple['type'] = line['SRC_CLMN_TYPE'] + ' ' + line['SRC_CLMN_LENGHT']
            clmnSimple['bk'] = 'false'
            clmnSimple['source'][0]['serial'] = str(clmnCnt)
            if len(line['SOURCECOLUMNNAMEFULL'].split(sep='.')) == 3:
                clmnSimple['source'][0]['alias'] = line['SOURCECOLUMNNAMEFULL'].split(sep='.')[2] + '_src'
                clmnSimple['source'][0]['column'] = line['SOURCECOLUMNNAMEFULL'].split(sep='.')[2]
            elif len(line['SOURCECOLUMNNAMEFULL'].split(sep='.')) == 2:
                clmnSimple['source'][0]['alias'] = line['SOURCECOLUMNNAMEFULL'].split(sep='.')[1] + '_src'
                clmnSimple['source'][0]['column'] = line['SOURCECOLUMNNAMEFULL'].split(sep='.')[1]
            else:
                clmnSimple['source'][0]['alias'] = 'INVALID VALUE'
                clmnSimple['source'][0]['column'] = 'INVALID VALUE'
            clmnToAppend = copy.deepcopy(clmnSimple)
            # add to mapping new column desc
            taskSimple['tasks'][transformCnt - 1]['mapping'].append(clmnToAppend)
            # print(jobCnt, transformCnt, line['FLOWNAME'], line['JOBNAME'], line['TRANSFORMNAME'])

            # taskFull['tasks'].append(taskSimple)
            # write to json for each new transform
            with open('./files/output/tst_mapping_csv.json', 'w') as jsonf:
                # json.dump(json.loads(json.dumps(clmnFull)), jsonf, indent=4, sort_keys=True)
                json.dump(taskSimple, jsonf, indent=4)
                jsonf.close()
            prev_transform = line['TRANSFORMNAME']
            prev_job = line['JOBNAME']
