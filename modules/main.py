from preparation import data_prep
from DDL import ddl
from add_extr_data import pipeline


def main():
    data_prep()
    ddl()
    # pipeline()


if __name__ == "__main__":
    main()
