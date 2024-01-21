import csv
import sys
import datetime as dt

def make_test_csv(in_fn, out_fn="test.csv", begin_date=None, end_date=None):

    if begin_date != None:
        begin_day = dt.datetime.strptime(begin_date, "%Y-%m-%d").date()
    if end_date != None:
        end_day = dt.datetime.strptime(end_date, "%Y-%m-%d").date()

    with open(in_fn, 'r') as f:
        linereader = csv.reader(f)
        firstline = True
        with open (out_fn, 'w') as out_f:

            writer = csv.writer(out_f)
            for row in linereader:
                if firstline:
                    firstline=False
                    writer.writerow(row)
                else:
                    date = dt.datetime.strptime(row[2], "%m/%d/%Y %I:%M:%S %p").date()
                    if begin_date == None or date >= begin_day:
                        if end_date == None or date <= end_day:
                            writer.writerow(row)
                        


if __name__ == "__main__":
    begin_date = "2022-01-01"
    end_date = "2022-01-31"

    args = sys.argv

    make_test_csv(in_fn = args[1], begin_date=begin_date, end_date=end_date)

