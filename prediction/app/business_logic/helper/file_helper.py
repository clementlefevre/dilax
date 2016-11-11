import os


def get_file_path(store_name, fileDir):
    filename = os.path.join(
        fileDir, '../' + store_name)
    filename = os.path.abspath(os.path.realpath(filename))
    print filename
    return filename
