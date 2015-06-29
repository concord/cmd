import os

def test_filepath(filepath):
    path = os.path.dirname(os.path.realpath(__file__))
    return path + '/' + filepath

def create_temporary_file(path):
    """
    This method expects an absolute file path and creates an empty file in side
    of zero or more directories specified by the path contents. If a directory
    with the desired name already exists it will not be replaced.
    """
    # Save current working directory
    current_dir = os.getcwd()
    os.chdir('/')

    # Create list of path contents
    path_items = path.split('/')

    # Create directories for each string between '/' characters
    for item in path_items[1:-1]:
        if not os.path.exists(item):
            os.makedirs(item)
        os.chdir(item)

    # Create a new empty file with the last string in 'path_items'
    open(path_items[-1], 'a').close()

    # Change back to original dir
    os.chdir(current_dir)
