if __name__ == '__main__':
    dict = {'test_group' : ['b'], 'table_nm' : [], 'clmfull' : []}
    first = {'a' : 10}
    sec = {'name': 'C2C', 'length': '1;', 'type': '$', 'label': 'C2C'}
    full = {'tst':[]}
    tst = {'test_group' : ['b'], 'table_nm' : [], 'clmfull' : []}
    dict['test_group'].append(first)
    dict['test_group'].append(sec)
    dict['table_nm'].append({'tablenm' : 'privet!'})
    dict['table_nm'].append({'tablenm' : 'poka!'})
    dict['clmfull'].append({'col1': 'col1asdsa!'})
    dict['clmfull'].append({'col2': 'col2adafqr13!'})
    full['tst'].append(dict)
    print(full)
    tst['test_group'].append(first)
    tst['test_group'].append(sec)
    tst['table_nm'].append({'tablenm' : '12312pr123ivet!'})
    tst['table_nm'].append({'tablenm' : '12312po123ka!'})
    tst['clmfull'].append({'col1': '123213col1123123asdsa!'})
    tst['clmfull'].append({'col2': '123123co12312321l2adafqr13!'})
    full['tst'].append(tst)
    print(full)
