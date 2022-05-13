import copy
import json

if __name__ == '__main__':
    taskExtractTmplt = {
        "tasks": [
            {
                "task_id": "",
                "description": "extract ...",
                "type": "stg_oracle_odbc",
                "source": [{
                    "connection": "%s_prod_conn",
                    "path": "fcchs",
                    "entity": "...",
                    "alias": "..._src"
                }],
                "target": [
                    {
                        "connection": "edw_hdfs",
                        "path": "stg/raw/dwh",
                        "entity": "...",
                        "alias": "..._stg"
                    }],
                "mapping": []
            }]
    }
    task_full = copy.deepcopy(taskExtractTmplt)
    task_1 =  {
                "task_id": "1",
                "description": "t1 ...",
                "type": "t1",
                "source": [{
                    "connection": "%t1",
                    "path": "t1",
                    "entity": "..t1.",
                    "alias": "...t1"
                }],
                "target": [
                    {
                        "connection": "t1",
                        "path": "stg/t1/dwh",
                        "entity": "...",
                        "alias": "...t1"
                    }],
                "mapping": []
            };
    task_full['tasks'].append(task_1)
    task_2 =  {
                "task_id": "2",
                "description": "t2 ...",
                "type": "t2",
                "source": [{
                    "connection": "%t2",
                    "path": "t2",
                    "entity": "...",
                    "alias": "...t2"
                }],
                "target": [
                    {
                        "connection": "t2",
                        "path": "stg/t2/dwh",
                        "entity": "...t2",
                        "alias": "...t2"
                    }],
                "mapping": []
            };
    del task_full['tasks'][0]
    task_full['tasks'].append(task_2)
    print(task_full['tasks'])
    with open('./files/output/tst_example.json', 'w') as jsonf:
        # json.dump(json.loads(json.dumps(clmnFull)), jsonf, indent=4, sort_keys=True)
        json.dump(task_full, jsonf, indent=4)
        jsonf.close()