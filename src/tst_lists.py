if __name__ == '__main__':
    match ' libname privet b  a="3"'.split():
        case operator, libref, type_conn:
            print(f'{operator=}, {libref=}, {type_conn=}')
        case operator, libref, type_conn, path:
            print(f'{operator=}, {libref=}, {type_conn=}, {path=}')
            # print(len(path.split('"')))
            for i in range(0, len(path.split('"'))):
                print(path.split('"')[i])
        case _:
            print('wrong libname operator definition')