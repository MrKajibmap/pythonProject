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
    dict_of_key_words_mapping = ['_OUTPUT_col\d+_name']
    dict_job_nm = ['etls_jobName']
    dict_libname = ['LIBNAME']
    dict_input_table = ['_INPUT =']
    dict_output_table = ['_OUTPUT =']
    with open('./src/files/101_138_02_Extract_DGP_DWH_TRN_TYPE.sas', 'r', encoding='utf-16') as deploy:
        for line in deploy:
            # колонки таблицы источника и таблицы _FULL
            for i in range(0, len(dict_of_key_words_mapping)):
                if re.search(dict_of_key_words_mapping[i], line):
                    print(line.split(sep='=')[-1].split(sep=';')[0].strip())
                # job_nm
            if re.search(dict_job_nm[i], line):
                print(line.split(sep='=')[-1].split(sep='%nrquote(')[-1].split(sep=');')[0].strip())
            # libnames
            if re.search(dict_libname[0], line):
                print(line)
                for j in range(0, line.count(' ')+1):
                    if len(line.split(sep=';')[0].split(sep=' ')[j].strip()) > 0:
                        print(line.split(sep=' ')[j].split(sep=';')[0].strip())
            #input table
            if re.search(dict_input_table[0], line):
                print(line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[-1].strip())
            # output table
            if re.search(dict_output_table[0], line):
                print(line.split(sep='=')[-1].split(sep=';')[0].split(sep='.')[-1].strip())

