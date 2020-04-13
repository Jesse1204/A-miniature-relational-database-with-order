import os
import sys
import re
import time
from functools import cmp_to_key
from BTrees.OOBTree import OOBTreePy

class MyDataBase:
    def __init__(self):
        self.tables = {}
        self.column_dict = {}
        self.relop = {'=': lambda x,y: x==y, '!=': lambda x,y: x!=y, '>': lambda x,y: x>y, '>=': lambda x,y: x>=y, '<': lambda x,y: x<y, '<=': lambda x,y: x<=y}
        self.arithop = {'+': lambda x,y: x+y, '-': lambda x,y: x-y, '*': lambda x,y: x*y, '/': lambda x,y: x/y}
        self.index = {}
        self.parasNo = []

    # read commands from file
    def readFiles(self, fileName):
        with open(fileName, "r") as f:
            commands = f.readlines()
        
        # remove all comments
        commands = [c.split("//")[0].strip() for c in commands]
        commands = list(filter(None, commands))
        # print("commands: ", commands)

        for command in commands:
            command = command.strip()
            print(command)

            command_output = command

            command = list(filter(None, re.split(":=|\)|\(|\\s+|,|(=)|(!=)|(>)|(<)|(>=)|(<=)", command)))

            outputTable = command[0]
            function = command[1]

            if command[0] == 'outputtofile':
                self.outputtofile(command[1], command[2])
                print()
            elif command[0] == 'Hash':
                self.hashIndex(command[1], command[2])
                print()
            elif command[0] == 'Btree':
                self.btreeIndex(command[1],command[2])
                print()
            else:
                if function == 'select':
                    self.tables[outputTable] = self.select(command[2],command[3:])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}                    
                elif function == 'inputfromfile':
                    self.tables[outputTable] = self.inputfromfile(command[2])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}                    
                elif function == 'project':
                    self.tables[outputTable] = self.project(command[2],command[3:])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'join':
                    self.tables[outputTable] = self.join(command[2],command[3],command[4:])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'count':
                    self.tables[outputTable] = self.count(command[2])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'sum':
                    self.tables[outputTable] = self.sum(command[2],command[3])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'avg':
                    self.tables[outputTable] = self.avg(command[2],command[3])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'countgroup':
                    self.tables[outputTable] = self.countgroup(command[2],command[3],command[4:])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'sumgroup':
                    self.tables[outputTable] = self.sumgroup(command[2],command[3],command[4:])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'avggroup':
                    self.tables[outputTable] = self.avggroup(command[2],command[3],command[4:])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'sort':
                    self.tables[outputTable] = self.sort(command[2],command[3:])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'movsum':
                    self.tables[outputTable] = self.movsum(command[2],command[3],command[4])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'movavg':
                    self.tables[outputTable] = self.movavg(command[2],command[3],command[4])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}
                elif function == 'concat':
                    self.tables[outputTable] = self.concat(command[2],command[3])
                    self.column_dict[outputTable] = {name:i for i, name in enumerate(self.tables[outputTable][0])}

                print("%d rows in set\n" % (len(self.tables[outputTable]) - 1))

                # write to AllOperations file
                self.output_to_AllOperations(outputTable, command_output, self.tables[outputTable])
            

    # read data from file and insert to tables
    def inputfromfile(self, tableName):
        start_time = time.time()
        fileName = tableName + '.txt'
        with open(fileName, "r") as f:
            lines = f.readlines()
        table = []
        header = lines[0].strip().split('|')
        table.append(header)
        data = lines[1:]
        for line in data:
            line = line.strip().split('|')
            table.append(line)
        end_time = time.time()
        print("Input Files runs in: %s s" % (end_time - start_time))
        return table
    

    # select * from table where conditions
    def select(self, tableName, conditions):
        start_time = time.time()
        table = []
        table.append(self.tables[tableName][0])
        data = self.tables[tableName][1:]

        sign, ret = self.check_index_availability(tableName, conditions)
        if sign:
            # use index
            for idx in ret:
                table.append(data[idx])
        else:
            for row in data:
                if self.check_conditions(tableName, conditions, row):
                    table.append(row)
        end_time = time.time()
        print("Select runs in: %s s" % (end_time - start_time))
        return table


    # check whether could apply index on this conditions
    def check_index_availability(self, tableName, conditions):
        if "and" in conditions or "or" in conditions or len(conditions) != 3 or conditions[1] != '=' or tableName not in self.index:
            return False, []
        if conditions[2].isdigit():
            # a = 3
            if conditions[0] not in self.index[tableName]:
                return False, []
            else:
                key = float(conditions[2])
                if key not in self.index[tableName][conditions[0]]:
                    return True, []
                else:
                    return True, self.index[tableName][conditions[0]][key]
        elif conditions[0].isdigit():
            # 3 = a
            if conditions[2] not in self.index[tableName]:
                return False, []
            else:
                key = float(conditions[0])
                if key not in self.index[tableName][conditions[2]]:
                    return True, []
                else:
                    return True, self.index[tableName][conditions[2]][key]


    # check conditions on a row of data, split by "and" or "or"
    # accept a paras of conditions
    def check_conditions(self, tableName, conditions, data):
        if "and" in conditions:
            idx = conditions.index("and")
            if self.check_condition(tableName, conditions[:idx], data):
                return self.check_conditions(tableName, conditions[idx+1:], data)
            else:
                return False
        elif "or" in conditions:
            idx = conditions.index("or")
            if self.check_condition(tableName, conditions[:idx], data):
                return True
            else:
                return self.check_conditions(tableName, conditions[idx+1:], data)
        else:
            return self.check_condition(tableName, conditions, data)


    # check condition on a row of data
    # accept only one condition, without "and" and "or"
    def check_condition(self, tableName, paras, data):
        if len(paras) == 5:
            if paras[1] in self.arithop:
                # qty + 1 < 30
                left = self.arithop[paras[1]](float(data[self.find_col_idx(tableName, paras[0])]), float(paras[2]))
                right = float(paras[4])
                return self.relop[paras[3]](left, right)
            elif paras[3] in self.arithop:
                # 30 > qty + 1
                left = float(paras[0])
                right = self.arithop[paras[3]](float(data[self.find_col_idx(tableName, paras[2])]), float(paras[4]))
                return self.relop[paras[1]](left, right)
        else:
            if paras[0] in self.column_dict[tableName]:
                # qty < 30
                left = data[self.find_col_idx(tableName, paras[0])]
                right = paras[2]
            else:
                # 30 > qty                
                left = paras[0]
                right = data[self.find_col_idx(tableName, paras[2])]

            if left.isdigit():
                left = float(left)
                right = float(right)

            return self.relop[paras[1]](left, right)


    # find the index of table.column, return the index, return type: int 
    def find_col_idx(self, tableName, columnName):
        return self.column_dict[tableName][columnName]


    # select attributes from table
    def project(self, tableName, attributes):
        start_time = time.time()
        table = []
        table.append(attributes)
        data = self.tables[tableName][1:]
        for row in data:
            line = []
            for attr in attributes:
                line.append(row[self.find_col_idx(tableName, attr)])
            table.append(line)
        end_time = time.time()
        print("Project runs in: %s s" % (end_time - start_time))
        return table


    # select * from table1, table2 where conditions
    # Within join there may be several conditions but only all ands
    def join(self, table1, table2, conditions):
        start_time = time.time()
        table = []

        header = []
        for t in self.tables[table1][0]:
            header.append(table1 + "_" + t)
        for t in self.tables[table2][0]:
            header.append(table2 + "_" + t)
        table.append(header)

        ret, data = self.check_index_availability_join(table1, table2, conditions)
        if ret:
            # use index
            table.extend(data)
            # print("using Index!!!")
        else:
            data1 = self.tables[table1][1:]
            data2 = self.tables[table2][1:]
            for d1 in data1:
                for d2 in data2:
                    if self.check_conditions_double(table1, table2, conditions, d1, d2):
                        line = d1
                        line.extend(d2)
                        table.append(line)
        end_time = time.time()
        print("Join runs in: %s s" % (end_time - start_time))
        return table


    # check index availability on two tables
    def check_index_availability_join(self, t1, t2, conditions):
        if "and" in conditions or "or" in conditions or len(conditions) != 3 or conditions[1] != '=':
            return False, []

        para = conditions[0].split('.')
        if para[0] == t1:
            att1 = para[1]
            att2 = conditions[2].split('.')[1]
        elif para[0] == t2:
            att1 = conditions[2].split('.')[1]
            att2 = para[1]

        data = []
        data1 = self.tables[t1][1:]
        data2 = self.tables[t2][1:]

        if t1 in self.index and att1 in self.index[t1] and t2 in self.index and att2 in self.index[t2]:
            for key in self.index[t1][att1]:
                if key in self.index[t2][att2]:
                    for idx1 in self.index[t1][att1][key]:
                        for idx2 in self.index[t2][att2][key]:
                            row = data1[idx1]
                            row.extend(data2[idx2])
                            data.append(row)
            return True, data
        else:
            return False, []


    # check conditions on two tables
    def check_conditions_double(self, table1, table2, conditions, row1, row2):
        if "and" in conditions:
            idx = conditions.index("and")
            if self.check_condition_double(table1, table2, conditions[:idx], row1, row2):
                return self.check_conditions_double(table1, table2, conditions[idx+1:], row1, row2)
            else:
                return False
        elif "or" in conditions:
            idx = conditions.index("or")
            if self.check_condition_double(table1, table2, conditions[:idx], row1, row2):
                return True
            else:
                return self.check_conditions_double(table1, table2, conditions[idx+1:], row1, row2)
        else:
            return self.check_condition_double(table1, table2, conditions, row1, row2)


    # check a condition on two tables
    def check_condition_double(self, table1, table2, paras, row1, row2):
        if len(paras) == 7:
            # a.s + 1 < b.s + 1
            left = self.arithop[paras[1]](float(self.get_corresponding_data(table1, table2, row1, row2, paras[0])), float(paras[2]))
            right = self.arithop[paras[5]](float(self.get_corresponding_data(table1, table2, row1, row2, paras[4])), float(paras[6]))
            return self.relop[paras[3]](left, right)
        elif len(paras) == 5:
            if paras[1] in self.arithop:
                # a.s + 1 < b.s
                left = self.arithop[paras[1]](float(self.get_corresponding_data(table1, table2, row1, row2, paras[0])), float(paras[2]))
                right = float(paras[4])
                return self.relop[paras[3]](left, right)
            elif paras[3] in self.arithop:
                # a.s > b.s + 1
                left = float(paras[0])
                right = self.arithop[paras[3]](float(self.get_corresponding_data(table1, table2, row1, row2, paras[2])), float(paras[4]))
                return self.relop[paras[1]](left, right)
        else:
            # a.s < b.s
            left = self.get_corresponding_data(table1, table2, row1, row2, paras[0])
            right = self.get_corresponding_data(table1, table2, row1, row2, paras[2])

            if left.isdigit():
                left = float(left)
                right = float(right)
            return self.relop[paras[1]](left, right)


    # get the corresponding data from column_dict, given a string patt, like: R.salesID
    def get_corresponding_data(self, table1, table2, row1, row2, patt):
        patt = patt.split('.')
        table = patt[0]
        attr = patt[1]
        if table == table1:
            return row1[self.column_dict[table][attr]]
        return row2[self.column_dict[table][attr]]        


    # select count(*) from table
    # count the number of rows in the table even if duplicate rows
    def count(self, tableName):
        start_time = time.time()
        table = [[tableName]]
        table.append([str(len(self.tables[tableName][1:]))])
        end_time = time.time()
        print("Count runs in: %s s" % (end_time - start_time))
        return table


    # select sum(agg_attr) from table
    def sum(self, tableName, agg_attr):
        start_time = time.time()
        table = [[tableName]]
        data = self.tables[tableName][1:]
        sum = 0
        for row in data:
            sum += float(row[self.find_col_idx(tableName, agg_attr)])
        table.append([str(sum)])
        end_time = time.time()
        print("Sum runs in: %s s" % (end_time - start_time))
        return table


    # select avg(agg_attr) from table
    def avg(self, tableName, agg_attr):
        start_time = time.time()
        table = [[tableName]]
        data = self.tables[tableName][1:]
        sum = 0
        for row in data:
            sum += float(row[self.find_col_idx(tableName, agg_attr)])
        length = len(self.tables[tableName][1:])
        if length==0:
            average = 0
        else:
            average = sum / length
        table.append([str(average)])
        end_time = time.time()
        print("Average runs in: %s s" % (end_time - start_time))
        return table
        
    
    # select count(agg_attr), attributes from table group by agg_attr
    # count the number of rows in the table even if duplicate rows
    def countgroup(self, tableName, agg_attr, attributes):
        start_time = time.time()
        table = []
        header = ["count(" + agg_attr + ")"]
        header.extend(attributes)
        table.append(header)
        data = self.tables[tableName][1:]
        group = {}
        for row in data:
            key = ""
            for attr in attributes:
                key += row[self.find_col_idx(tableName, attr)] + ' '
            if key not in group:
                group[key] = 0
            group[key] += 1

        for key in group:
            cols = key.strip().split(' ')
            line = [str(group[key])]
            line.extend(cols)
            table.append(line)
        end_time = time.time()
        print("Countgroup runs in: %s s" % (end_time - start_time))
        return table


    # select sum(agg_attr), attributes from table group by agg_attr
    def sumgroup(self, tableName, agg_attr, attributes):
        start_time = time.time()
        table = []
        header = ["sum(" + agg_attr + ")"]
        header.extend(attributes)
        table.append(header)
        data = self.tables[tableName][1:]
        group = {}
        for row in data:
            key = ""
            for attr in attributes:
                key += row[self.find_col_idx(tableName, attr)] + ' '
            if key not in group:
                group[key] = 0
            group[key] += float(row[self.find_col_idx(tableName, agg_attr)])

        for key in group:
            cols = key.strip().split(' ')
            line = [str(group[key])]
            line.extend(cols)
            table.append(line)
        end_time = time.time()
        print("Sumgroup runs in: %s s" % (end_time - start_time))
        # print(table)
        return table

    
    # select avg(agg_attr), attributes from table group by agg_attr
    def avggroup(self, tableName, agg_attr, attributes):
        start_time = time.time()
        table = []
        header = ["avg(" + agg_attr + ")"]
        header.extend(attributes)
        table.append(header)
        data = self.tables[tableName][1:]
        group = {}
        for row in data:
            key = ""
            for attr in attributes:
                key += row[self.find_col_idx(tableName, attr)] + ' '
            if key not in group:
                group[key] = [0, 0]
            group[key][0] += 1
            group[key][1] += float(row[self.find_col_idx(tableName, agg_attr)])

        for key in group:
            cols = key.strip().split(' ')
            line = [str(group[key][1] / group[key][0])]
            line.extend(cols)
            table.append(line)
        end_time = time.time()
        print("Averagegroup runs in: %s s" % (end_time - start_time))
        return table


    # compare function, used by sort
    def compare(self, row1, row2):
        for i in self.parasNo:
            if row1[i] < row2[i]:
                return -1
            elif row1[i] > row2[i]:
                return 1
        return 0


    # sort table by attributes (in order)
    def sort(self, tableName, attributes):
        start_time = time.time()
        header = self.tables[tableName][0]
        table = [header]
        data = self.tables[tableName][1:]
        self.parasNo = [self.find_col_idx(tableName, attr) for attr in attributes]
        sorted_data = sorted(data, key=cmp_to_key(self.compare))
        table.extend(sorted_data)
        end_time = time.time()
        print("Sort runs in: %s s" % (end_time - start_time))
        return table


    # perform moving sum of table on attr of size 
    def movsum(self, tableName, attr, size):
        start_time = time.time()
        size = int(size)
        header = self.tables[tableName][0]
        table = [header]
        data = self.tables[tableName][1:]
        idx = self.find_col_idx(tableName, attr)
        prefix = 0
        for i, d in enumerate(data):
            row = d.copy()
            prefix += float(row[idx])
            if i >= size:
                prefix -= float(data[i-size][idx])
            row[idx] = prefix
            table.append(row)
        end_time = time.time()
        print("Movsum runs in: %s s" % (end_time - start_time))
        return table


    # perform moving average of table on attr of size 
    def movavg(self, tableName, attr, size):
        start_time = time.time()
        size = int(size)
        header = self.tables[tableName][0]
        table = [header]
        data = self.tables[tableName][1:]
        idx = self.find_col_idx(tableName, attr)
        prefix = 0
        for i, d in enumerate(data):
            row = d.copy()
            prefix += float(row[idx])
            if i >= size:
                prefix -= float(data[i-size][idx])
                row[idx] = prefix / size
            else:
                row[idx] = prefix / (i + 1)
            table.append(row)
        # print(table[:10])
        end_time = time.time()
        print("Moving Average runs in: %s s" % (end_time - start_time))
        return table


    # concatenate the two tables (must have the same schema)
    # Duplicate rows may result
    def concat(self, table1, table2):
        start_time = time.time()
        header = self.tables[table1][0]
        table = [header]
        data = self.tables[table1][1:]
        for row in data:
            table.append(row)
        data = self.tables[table2][1:]
        for row in data:
            table.append(row)
        end_time = time.time()
        print("Concat runs in: %s s" % (end_time - start_time))
        return table


    # create hash index on attr of table
    # Index is like: (attr_val, idx of array table)
    def hashIndex(self, tableName, attr):
        start_time = time.time()
        hashIdx = {}
        data = self.tables[tableName][1:]
        i = self.find_col_idx(tableName, attr)
        for idx, row in enumerate(data):
            key = float(row[i])
            if key not in hashIdx:
                hashIdx[key] = []
            hashIdx[key].append(idx)
        if tableName not in self.index:
            self.index[tableName] = {}
        self.index[tableName][attr] = hashIdx
        # print(hashIdx)
        end_time = time.time()
        print("Hash runs in: %s s" % (end_time - start_time))


    # create btree index on attr of table
    # Index is like: (attr_val, idx of array table)
    def btreeIndex(self, tableName, attr):
        start_time = time.time()
        btreeIdx = OOBTreePy()
        data = self.tables[tableName][1:]
        i = self.find_col_idx(tableName, attr)
        for idx, row in enumerate(data):
            key = float(row[i])
            if key not in btreeIdx:
                btreeIdx[key] = []
            btreeIdx[key].append(idx)
        if tableName not in self.index:
            self.index[tableName] = {}
        self.index[tableName][attr] = btreeIdx
        # print(btreeIdx)
        end_time = time.time()
        print("B tree runs in: %s s" % (end_time - start_time))


    # output table data into file
    def outputtofile(self, table, fileName):
        start_time = time.time()
        output_path = "output/"
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        fileName = os.path.join(output_path, 'YL6569_ZC1717_' + fileName + '.txt')
        with open(fileName, "w") as f:
            for line in self.tables[table]:
                for idx, data in enumerate(line):
                    if idx == len(line) - 1:
                        f.write(data + "\n")
                    else:
                        f.write(data + "|")
        end_time = time.time()
        print("Output runs in: %s s" % (end_time - start_time))


    # output results to AllOperations file
    def output_to_AllOperations(self, tableName, command, table):
        output_path = "output/"
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        fileName = os.path.join(output_path, 'YL6569_ZC1717_AllOperations.txt')
        with open(fileName, "a") as f:
            f.write(tableName + '\n')
            f.write(command + '\n')
            for line in table:
                # print(line)
                for idx, data in enumerate(line):
                    if idx == len(line) - 1:
                        f.write(data + "\n")
                    else:
                        f.write(data + "|")
            f.write(str(len(table)-1) + " rows\n")
            f.write('\n')


if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        input_path = "test.txt"

    test = MyDataBase()
    test.readFiles(input_path)
