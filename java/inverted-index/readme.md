# Variant 3. Inverted Index

## Sample Input File

    Doc1=This is sample input file
    Doc2=Each line presents separate document
    Doc3=Part before the equal sign is the name of the document
    Doc4=Part after the equal sign is the text of document

## Output from the MapReduce Task

    Each	(Doc2 0)
    Part	(Doc3 0) (Doc4 0)
    This	(Doc1 0)
    after	(Doc4 5)
    before	(Doc3 5)
    document	(Doc4 41) (Doc2 28) (Doc3 46)
    equal	(Doc3 16) (Doc4 15)
    file	(Doc1 21)
    input	(Doc1 15)
    is	(Doc3 27) (Doc4 26) (Doc1 5)
    line	(Doc2 5)
    name	(Doc3 34)
    of	(Doc4 38) (Doc3 39)
    presents	(Doc2 10)
    sample	(Doc1 8)
    separate	(Doc2 19)
    sign	(Doc3 22) (Doc4 21) 
    text	(Doc4 33)
    the	(Doc3 42) (Doc3 30) (Doc4 11) (Doc3 12) (Doc4 29)


   
  
