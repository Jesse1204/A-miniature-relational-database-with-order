A miniature relational database with order implemented by Yucong Liu and Ziheng Cao. 
The program imports BTree from BTrees.OOBTree. Hash and BTree structure was slightly 
modifed from the previous Homework.Hash structure is actually the dictionary.

You can refer to the project_description.pdf for the details.

To execute the program, run  "database.py". The default input file is "test.txt",
After the execution you will see "AllOperations" in the output folders.

You can execute
$ python database.py <filename>.txt
in the terminal or command line to run the program. <filename>.txt is the input file name.

For example, 
$ python database.py commands.txt

We include a smaller command input, called "test.txt". Feel free to switch to any command input you want.
We also include a large command input, called "commands.txt".

You can also change the input file name by following: 
1.modify the the test.txt under the same directory, 
2.change the file name in "main" method to be the name of your input file.

After you decompress the reprozip, source files will be at $ {unpack dir}/root/mnt/project



Expected shortcoming:

1. 'Join' may be too slow on large dataset. But it would be much faster if you switch to a smaller dataset.

2. If you execute the program on cims server, because the OOBTree is not imported, it may cause
an error. 
