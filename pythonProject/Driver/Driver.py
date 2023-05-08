from sys import argv
from processing import pre_processing,aggregation

def main():
    """
    :return:
    Driver Class Main method which parses the command line arguments and trigger processing according to stage.
    """

    if len(argv) > 3:
        raise Exception("Required arguments: 3 but received {}".format(len(argv)))
    else:
        env = argv[1]
        config_location = argv[2] #if len(argv) == 4 else ""
        pre_processing.main(config_location,env)
        aggregation.main(config_location,env)



if __name__ == '__main__':
    main()
