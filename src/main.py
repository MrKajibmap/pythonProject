# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
# from files import '101_138_02_Extract_DGP_DWH_TRN_TYPE.sas'
import re


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dict_of_key_words = ['etl_job_start', 'etl_job_finish']
    dict_of_key_words_mapping = ['\Wlet _OUTPUT_col\d{1,}_name']
    dict_job_nm = ['etls_jobName']
    dict_libname = ['LIBNAME']
    dict_input_table = ['_INPUT =']
    dict_output_table = ['_OUTPUT =']
    libraries = []
    clmnFull = {}
    with open('./src/files/101_138_02_Extract_DGP_DWH_TRN_TYPE.sas', 'r', encoding='utf-16') as deploy:
        for line in deploy:
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
                # print('first input of clmn num = ', line.split(sep='col')[1].split(sep='_')[0].strip())
                # print('ClmnPos = ', line.split(sep='col')[1].split(sep='_')[0].strip(), 'clmnNm = ',
                #       line.split(sep='=')[-1].split(sep=';')[0].strip())
                # clmnNm = {'clmnNm': line.split(sep='=')[-1].split(sep=';')[0].strip()}
                # print(clmnNm)
                clmnFull['clmnNm'] = line.split(sep='=')[-1].split(sep=';')[0].strip()
                print(clmnFull)
            if re.search('\Wlet _OUTPUT_col\d{1,}_length =', line):
                # print('ClmnPos = ', line.split(sep='col')[1].split(sep='_')[0].strip(), 'length = ',
                #       line.split(sep='=')[-1].strip())
                # clmnLngth = {'clmnLngth': line.split(sep='=')[-1].strip()}
                # print(clmnLngth)
                clmnFull['clmnLngth'] = line.split(sep='=')[-1].strip()
                print(clmnFull)
            if re.search('\Wlet _OUTPUT_col\d{1,}_type', line):
                # print('ClmnPos = ', line.split(sep='col')[1].split(sep='_')[0].strip(), 'type = ',
                #       line.split(sep='=')[-1].split(sep=';')[0].strip())
                # clmnType = {'clmnType': line.split(sep='=')[-1].split(sep=';')[0].strip()}
                # print(clmnType)
                clmnFull['clmnType'] = line.split(sep='=')[-1].split(sep=';')[0].strip()
                print(clmnFull)
            if re.search('\Wlet _OUTPUT_col\d{1,}_label =', line):
                # print('ClmnPos = ', line.split(sep='col')[1].split(sep='_')[0].strip(), 'label = ',
                #       line.split(sep='=')[-1].split(sep=')')[0].split(sep='(')[-1].strip())
                # clmnLabel = {'clmnType': line.split(sep='=')[-1].split(sep=')')[0].split(sep='(')[-1].strip()}
                # print(clmnLabel)
                clmnFull['clmnLabel'] = line.split(sep='=')[-1].split(sep=')')[0].split(sep='(')[-1].strip()
                print(clmnFull)

    print(list(libraries))
