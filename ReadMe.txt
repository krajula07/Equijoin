Mapper:
this function makes the key value pairs 
Reducer:
gets the values that matches with the key as iterator and combines the table which gives the final output which writes the output file.
Driver:
which is the main class and calls all the functions to get the job done.

The input which we give will be taken in mapper and every row of data will be divided into key value pairs where key is the joining column value and value is the whole row data.
In the intermediate stage all the matching keys will be combined and the respective values will be stored in an iterable and sent to a reducer.In the Reducer stage the data from the intermediate stage is 
divided into two tables like table 1 and table 2 ,then a nested loop is used to combine two table and to get the output table.



